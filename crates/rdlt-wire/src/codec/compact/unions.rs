//! Rebuilds unions and run-end columns from the rows named: each child of a dense union holds
//! the items its rows name, a run-end column the runs its rows are in.

use std::sync::Arc;

use arrow_array::types::RunEndIndexType;
use arrow_array::{Array as _, ArrayRef, PrimitiveArray, RunArray, UnionArray, make_array};
use arrow_buffer::ArrowNativeType as _;
use arrow_schema::{ArrowError, DataType};

use super::{Narrower, Ranges, name, sized};

fn beyond(what: &str, at: usize) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("{what} {at} is beyond the column"))
}

impl Narrower {
    /// Unions: the same rows of each child of a sparse one, and of a dense one the item each
    /// row names, in the rows' order.
    pub(super) fn unions(
        &mut self,
        union: &UnionArray,
        ranges: &Ranges,
    ) -> Result<ArrayRef, ArrowError> {
        let DataType::Union(fields, _) = union.data_type() else {
            return Ok(Arc::new(union.clone()));
        };
        let rows = || ranges.iter().flat_map(|(start, end)| *start..*end);
        let mut ids = Vec::new();
        for row in rows() {
            ids.push(
                *union
                    .type_ids()
                    .get(row)
                    .ok_or_else(|| beyond("row", row))?,
            );
        }
        let Some(offsets) = union.offsets() else {
            let children = fields.iter();
            let children: Result<Vec<_>, _> = children
                .map(|(id, _)| self.gathered(union.child(id), ranges))
                .collect();
            let union = UnionArray::try_new(fields.clone(), ids.into(), None, children?)?;
            return Ok(Arc::new(union));
        };
        let mut named = vec![Vec::new(); fields.len()];
        let mut counts = vec![0_usize; fields.len()];
        let mut moved = Vec::with_capacity(ids.len());
        for (row, id) in rows().zip(&ids) {
            let child = fields.iter().position(|(of, _)| of == *id);
            let child = child.ok_or_else(|| beyond("the child of row", row))?;
            let at = usize::try_from(offsets[row]).map_err(|_| beyond("the item of row", row))?;
            name(&mut named[child], at, at.saturating_add(1));
            moved.push(sized::<i32>(counts[child])?);
            counts[child] += 1;
        }
        let mut children = Vec::with_capacity(fields.len());
        for ((id, _), named) in fields.iter().zip(&named) {
            children.push(self.gathered(union.child(id), named)?);
        }
        let union = UnionArray::try_new(fields.clone(), ids.into(), Some(moved.into()), children)?;
        Ok(Arc::new(union))
    }

    /// Run-end columns: the runs the rows named are in, a run for each stretch of rows in one
    /// run, ending where the rows count them.
    pub(super) fn runs<R: RunEndIndexType>(
        &mut self,
        runs: &RunArray<R>,
        ranges: &Ranges,
    ) -> Result<ArrayRef, ArrowError>
    where
        R::Native: TryFrom<usize>,
    {
        let (buffer, offset) = (runs.run_ends(), runs.run_ends().offset());
        let (mut ends, mut values, mut total) = (Vec::<R::Native>::new(), Vec::new(), 0_usize);
        let mut last = None;
        for (start, end) in ranges.iter().filter(|(start, end)| start < end) {
            let (mut at, mut run) = (*start, buffer.get_physical_index(*start));
            while at < *end {
                let reach = buffer.values().get(run).ok_or_else(|| beyond("row", at))?;
                let upto = reach.as_usize().saturating_sub(offset).min(*end);
                if upto <= at {
                    return Err(beyond("row", at));
                }
                total = total.saturating_add(upto - at);
                if last == Some(run) {
                    ends.pop();
                } else {
                    name(&mut values, run, run + 1);
                    last = Some(run);
                }
                ends.push(sized::<R::Native>(total)?);
                (at, run) = (upto, run + 1);
            }
        }
        let ends = PrimitiveArray::<R>::from_iter_values(ends);
        let values = self.gathered(runs.values(), &values)?;
        // Rebuilt under the column's own type: its fields may be named, nullable or described
        // otherwise than a new run-end array's.
        let rebuilt = runs
            .to_data()
            .into_builder()
            .offset(0)
            .len(total)
            .child_data(vec![ends.into_data(), values.into_data()]);
        Ok(make_array(rebuilt.build()?))
    }
}
