//! The dialect a SQL transform's text is read in: sqlparser's generic dialect, but for the
//! keywords whose own syntax holds expressions, which it reads one way alone, queries naming their
//! table first, and a bound on the reading itself.

mod reach;
#[cfg(test)]
mod tests;

use std::any::TypeId;
use std::cell::Cell;

use datafusion_sql::sqlparser::ast::{Expr, ObjectName};
use datafusion_sql::sqlparser::dialect::{Dialect, GenericDialect};
use datafusion_sql::sqlparser::keywords::{Keyword, RESERVED_FOR_IDENTIFIER};
use datafusion_sql::sqlparser::parser::{Parser, ParserError};
use datafusion_sql::sqlparser::tokenizer::{Token, TokenWithSpan};

use self::reach::{charges, spaced};

/// sqlparser's generic dialect, every rule of it forwarded, but for [`NESTING`], `POSITION(`,
/// [`NESTED`] types and `INTERVAL` where an operand stands, a query read with its table first,
/// and a budget of work.
#[derive(Debug)]
pub(crate) struct Guarded {
    /// Tokens the parser may spend, spent again where it backtracks, before the text is refused.
    reads: usize,
    /// Tokens it has spent.
    spent: Cell<usize>,
    /// Why the text is refused, where it is.
    refused: Cell<Option<Refused>>,
    /// What a read at each token costs.
    charges: Vec<usize>,
}

/// Why a text's reading is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refused {
    /// The parser spent more than the text's length allows.
    Work,
    /// The text opens a nested type where an operand stands.
    Nested,
    /// The text holds an interval where an operand stands.
    Interval,
}

/// Tokens the parser may spend for each token of the text, spaces aside, backtracks included: a
/// read costs one, and every token a typed string's type tried there may take.
///
/// No query the tests admit, the drawn differential's among them, spends 6.5 a token: 64 `NOT`s,
/// each charged the words after it, spend the most. Four of them before a call of five thousand
/// arguments spend 6.5 too, as a list is charged by each of the five reads at most that reach
/// it; a long select list, `IN` list or `CASE` spends 1.6 at most, and nested listed calls 2.5.
/// Sixteen leave room for any reading of an admitted query, while text read again a level is
/// refused once it spends sixteen a token.
const READS: usize = 16;

/// Words a typed string's type may take beside its name and its parenthesised part, as in
/// `TIMESTAMP(3) WITH TIME ZONE`, `CHARACTER VARYING` or `INT UNSIGNED`.
const TYPE_WORDS: usize = 4;

/// Types that hold other types, opened by `(` or `<`, and so may hold expressions.
///
/// Where an operand stands, sqlparser reads each first as a typed string's type and then again
/// as its own syntax, a call or a comparison, so a nest of them is read twice a level, and the
/// type alone again for every `ARRAY<` of a nest. No admitted query holds one.
const NESTED: [Keyword; 9] = [
    Keyword::ARRAY,
    Keyword::LOWCARDINALITY,
    Keyword::MAP,
    Keyword::NESTED,
    Keyword::NULLABLE,
    Keyword::STRUCT,
    Keyword::TABLE,
    Keyword::TUPLE,
    Keyword::UNION,
];

impl Guarded {
    /// The dialect for reading `tokens`.
    pub(crate) fn reading(tokens: &[TokenWithSpan]) -> Self {
        let words = tokens.iter().filter(|token| !spaced(&token.token)).count();
        Self {
            reads: words.saturating_mul(READS),
            spent: Cell::new(0),
            refused: Cell::new(None),
            charges: charges(tokens),
        }
    }

    /// Why the text read is refused, where it is.
    pub(crate) fn refused(&self) -> Option<Refused> {
        self.refused.get()
    }

    /// Refuses the text for `why`: every read from here on fails at once, with an error no
    /// backtrack takes another path past.
    fn refuse(&self, why: Refused) -> Result<Expr, ParserError> {
        if self.refused.get().is_none() {
            self.refused.set(Some(why));
        }
        Err(ParserError::RecursionLimitExceeded)
    }
}

/// Keywords whose own syntax holds expressions and which sqlparser, where that syntax fails,
/// reads again as a function's name.
///
/// Text failing inside a nest of them is read twice a level, so its parse takes time two to the
/// power of the levels: 22 nested `CAST`s broken at the innermost took five seconds. Each is
/// read as its own syntax alone, a name only quoted.
const NESTING: [Keyword; 18] = [
    Keyword::ARRAY,
    Keyword::CASE,
    Keyword::CAST,
    Keyword::CEIL,
    Keyword::CONVERT,
    Keyword::CURRENT_DATE,
    Keyword::CURRENT_TIME,
    Keyword::CURRENT_TIMESTAMP,
    Keyword::FLOOR,
    Keyword::LOCALTIME,
    Keyword::LOCALTIMESTAMP,
    Keyword::NOT,
    Keyword::OVERLAY,
    Keyword::SAFE_CAST,
    Keyword::SUBSTR,
    Keyword::SUBSTRING,
    Keyword::TRY_CAST,
    Keyword::TRY_CONVERT,
];

impl Dialect for Guarded {
    fn dialect(&self) -> TypeId {
        TypeId::of::<GenericDialect>()
    }

    fn is_reserved_for_identifier(&self, kw: Keyword) -> bool {
        RESERVED_FOR_IDENTIFIER.contains(&kw) || NESTING.contains(&kw)
    }

    /// Charges each operand read, refusing the text past its budget, where a [`NESTED`] type
    /// opens or where an interval stands; and reads `POSITION(` as a call alone: sqlparser
    /// reads it as `POSITION(a IN b)` first and, where that fails, as a call, so a nest of them,
    /// read thrice a level, took twenty seconds at 24 levels.
    ///
    /// sqlparser takes `INTERVAL` as a typed string's type and reads its operand there, then,
    /// where that fails, reads it again as its own syntax: twenty of them before a 16 KiB list
    /// took sixteen seconds, the time growing as the square of the text. No admitted query holds
    /// an interval, and no other keyword is read so.
    fn parse_prefix(&self, parser: &mut Parser<'_>) -> Option<Result<Expr, ParserError>> {
        if self.refused.get().is_some() {
            return Some(Err(ParserError::RecursionLimitExceeded));
        }
        let charge = self.charges.get(parser.index()).copied().unwrap_or(1);
        let spent = self.spent.get().saturating_add(charge);
        self.spent.set(spent);
        if spent > self.reads {
            return Some(self.refuse(Refused::Work));
        }
        let Token::Word(word) = &parser.peek_token_ref().token else {
            return None;
        };
        let keyword = word.keyword;
        if keyword == Keyword::INTERVAL {
            return Some(self.refuse(Refused::Interval));
        }
        let next = &parser.peek_nth_token_ref(1).token;
        if NESTED.contains(&keyword) && matches!(next, Token::LParen | Token::Lt) {
            return Some(self.refuse(Refused::Nested));
        }
        if keyword != Keyword::POSITION || *next != Token::LParen {
            return None;
        }
        let TokenWithSpan {
            token: Token::Word(word),
            span,
        } = parser.next_token()
        else {
            return None;
        };
        Some(parser.parse_function(ObjectName::from(vec![word.into_ident(span)])))
    }

    /// No query is read with its table first, as `FROM input SELECT n`: sqlparser reads each
    /// parenthesised table as such a query first and then as a join, so a nest of `FROM (`
    /// was read again a level, its work growing as the square of the nest's depth.
    fn supports_from_first_select(&self) -> bool {
        false
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        GenericDialect.is_delimited_identifier_start(ch)
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        GenericDialect.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        GenericDialect.is_identifier_part(ch)
    }

    fn supports_unicode_string_literal(&self) -> bool {
        GenericDialect.supports_unicode_string_literal()
    }

    fn supports_partition_by_after_order_by(&self) -> bool {
        GenericDialect.supports_partition_by_after_order_by()
    }

    fn supports_array_join_syntax(&self) -> bool {
        GenericDialect.supports_array_join_syntax()
    }

    fn supports_group_by_expr(&self) -> bool {
        GenericDialect.supports_group_by_expr()
    }

    fn supports_group_by_with_modifier(&self) -> bool {
        GenericDialect.supports_group_by_with_modifier()
    }

    fn supports_left_associative_joins_without_parens(&self) -> bool {
        GenericDialect.supports_left_associative_joins_without_parens()
    }

    fn supports_connect_by(&self) -> bool {
        GenericDialect.supports_connect_by()
    }

    fn supports_match_recognize(&self) -> bool {
        GenericDialect.supports_match_recognize()
    }

    fn supports_pipe_operator(&self) -> bool {
        GenericDialect.supports_pipe_operator()
    }

    fn supports_start_transaction_modifier(&self) -> bool {
        GenericDialect.supports_start_transaction_modifier()
    }

    fn supports_window_function_null_treatment_arg(&self) -> bool {
        GenericDialect.supports_window_function_null_treatment_arg()
    }

    fn supports_dictionary_syntax(&self) -> bool {
        GenericDialect.supports_dictionary_syntax()
    }

    fn supports_window_clause_named_window_reference(&self) -> bool {
        GenericDialect.supports_window_clause_named_window_reference()
    }

    fn supports_parenthesized_set_variables(&self) -> bool {
        GenericDialect.supports_parenthesized_set_variables()
    }

    fn supports_select_wildcard_except(&self) -> bool {
        GenericDialect.supports_select_wildcard_except()
    }

    fn support_map_literal_syntax(&self) -> bool {
        GenericDialect.support_map_literal_syntax()
    }

    fn allow_extract_custom(&self) -> bool {
        GenericDialect.allow_extract_custom()
    }

    fn allow_extract_single_quotes(&self) -> bool {
        GenericDialect.allow_extract_single_quotes()
    }

    fn supports_extract_comma_syntax(&self) -> bool {
        GenericDialect.supports_extract_comma_syntax()
    }

    fn supports_create_view_comment_syntax(&self) -> bool {
        GenericDialect.supports_create_view_comment_syntax()
    }

    fn supports_parens_around_table_factor(&self) -> bool {
        GenericDialect.supports_parens_around_table_factor()
    }

    fn supports_values_as_table_factor(&self) -> bool {
        GenericDialect.supports_values_as_table_factor()
    }

    fn supports_create_index_with_clause(&self) -> bool {
        GenericDialect.supports_create_index_with_clause()
    }

    fn supports_explain_with_utility_options(&self) -> bool {
        GenericDialect.supports_explain_with_utility_options()
    }

    fn supports_limit_comma(&self) -> bool {
        GenericDialect.supports_limit_comma()
    }

    fn supports_update_order_by(&self) -> bool {
        GenericDialect.supports_update_order_by()
    }

    fn supports_projection_trailing_commas(&self) -> bool {
        GenericDialect.supports_projection_trailing_commas()
    }

    fn supports_asc_desc_in_column_definition(&self) -> bool {
        GenericDialect.supports_asc_desc_in_column_definition()
    }

    fn supports_try_convert(&self) -> bool {
        GenericDialect.supports_try_convert()
    }

    fn supports_bitwise_shift_operators(&self) -> bool {
        GenericDialect.supports_bitwise_shift_operators()
    }

    fn supports_comment_on(&self) -> bool {
        GenericDialect.supports_comment_on()
    }

    fn supports_load_extension(&self) -> bool {
        GenericDialect.supports_load_extension()
    }

    fn supports_named_fn_args_with_assignment_operator(&self) -> bool {
        GenericDialect.supports_named_fn_args_with_assignment_operator()
    }

    fn supports_struct_literal(&self) -> bool {
        GenericDialect.supports_struct_literal()
    }

    fn supports_empty_projections(&self) -> bool {
        GenericDialect.supports_empty_projections()
    }

    fn supports_nested_comments(&self) -> bool {
        GenericDialect.supports_nested_comments()
    }

    fn supports_multiline_comment_hints(&self) -> bool {
        GenericDialect.supports_multiline_comment_hints()
    }

    fn supports_user_host_grantee(&self) -> bool {
        GenericDialect.supports_user_host_grantee()
    }

    fn supports_string_escape_constant(&self) -> bool {
        GenericDialect.supports_string_escape_constant()
    }

    fn supports_array_typedef_with_brackets(&self) -> bool {
        GenericDialect.supports_array_typedef_with_brackets()
    }

    fn supports_match_against(&self) -> bool {
        GenericDialect.supports_match_against()
    }

    fn supports_set_names(&self) -> bool {
        GenericDialect.supports_set_names()
    }

    fn supports_comma_separated_set_assignments(&self) -> bool {
        GenericDialect.supports_comma_separated_set_assignments()
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        GenericDialect.supports_filter_during_aggregation()
    }

    fn supports_select_wildcard_exclude(&self) -> bool {
        GenericDialect.supports_select_wildcard_exclude()
    }

    fn supports_data_type_signed_suffix(&self) -> bool {
        GenericDialect.supports_data_type_signed_suffix()
    }

    fn supports_interval_options(&self) -> bool {
        GenericDialect.supports_interval_options()
    }

    fn supports_quote_delimited_string(&self) -> bool {
        GenericDialect.supports_quote_delimited_string()
    }

    fn supports_select_wildcard_replace(&self) -> bool {
        GenericDialect.supports_select_wildcard_replace()
    }

    fn supports_select_wildcard_ilike(&self) -> bool {
        GenericDialect.supports_select_wildcard_ilike()
    }

    fn supports_select_wildcard_rename(&self) -> bool {
        GenericDialect.supports_select_wildcard_rename()
    }

    fn supports_optimize_table(&self) -> bool {
        GenericDialect.supports_optimize_table()
    }

    fn supports_install(&self) -> bool {
        GenericDialect.supports_install()
    }

    fn supports_detach(&self) -> bool {
        GenericDialect.supports_detach()
    }

    fn supports_prewhere(&self) -> bool {
        GenericDialect.supports_prewhere()
    }

    fn supports_with_fill(&self) -> bool {
        GenericDialect.supports_with_fill()
    }

    fn supports_limit_by(&self) -> bool {
        GenericDialect.supports_limit_by()
    }

    fn supports_interpolate(&self) -> bool {
        GenericDialect.supports_interpolate()
    }

    fn supports_settings(&self) -> bool {
        GenericDialect.supports_settings()
    }

    fn supports_select_format(&self) -> bool {
        GenericDialect.supports_select_format()
    }

    fn supports_comment_optimizer_hint(&self) -> bool {
        GenericDialect.supports_comment_optimizer_hint()
    }

    fn supports_constraint_keyword_without_name(&self) -> bool {
        GenericDialect.supports_constraint_keyword_without_name()
    }

    fn supports_key_column_option(&self) -> bool {
        GenericDialect.supports_key_column_option()
    }

    fn supports_comma_separated_trim(&self) -> bool {
        GenericDialect.supports_comma_separated_trim()
    }

    fn supports_cte_without_as(&self) -> bool {
        GenericDialect.supports_cte_without_as()
    }

    fn supports_select_item_multi_column_alias(&self) -> bool {
        GenericDialect.supports_select_item_multi_column_alias()
    }

    fn supports_xml_expressions(&self) -> bool {
        GenericDialect.supports_xml_expressions()
    }
}
