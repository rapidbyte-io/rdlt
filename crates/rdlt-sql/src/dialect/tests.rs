use std::cell::Cell;
use std::thread;
use std::time::{Duration, Instant};

use datafusion_sql::sqlparser::dialect::{Dialect, GenericDialect};
use datafusion_sql::sqlparser::keywords::ALL_KEYWORDS;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::sqlparser::tokenizer::Tokenizer;

use super::reach::charges;
use super::{Guarded, READS, Refused};
use crate::limits::PARSER_DEPTH;
use crate::{SQL_BYTES, Sql, SqlErrorKind, stack};

/// Each rule of the generic dialect that takes no character, by name, but whether a query may
/// name its table first, which the guarded dialect refuses.
#[expect(
    clippy::type_complexity,
    reason = "a rule is a name and the call that asks a dialect it"
)]
const RULES: [(&str, fn(&dyn Dialect) -> bool); 67] = [
    ("supports_unicode_string_literal", |dialect| {
        dialect.supports_unicode_string_literal()
    }),
    ("supports_partition_by_after_order_by", |dialect| {
        dialect.supports_partition_by_after_order_by()
    }),
    ("supports_array_join_syntax", |dialect| {
        dialect.supports_array_join_syntax()
    }),
    ("supports_group_by_expr", |dialect| {
        dialect.supports_group_by_expr()
    }),
    ("supports_group_by_with_modifier", |dialect| {
        dialect.supports_group_by_with_modifier()
    }),
    (
        "supports_left_associative_joins_without_parens",
        |dialect| dialect.supports_left_associative_joins_without_parens(),
    ),
    ("supports_connect_by", |dialect| {
        dialect.supports_connect_by()
    }),
    ("supports_match_recognize", |dialect| {
        dialect.supports_match_recognize()
    }),
    ("supports_pipe_operator", |dialect| {
        dialect.supports_pipe_operator()
    }),
    ("supports_start_transaction_modifier", |dialect| {
        dialect.supports_start_transaction_modifier()
    }),
    ("supports_window_function_null_treatment_arg", |dialect| {
        dialect.supports_window_function_null_treatment_arg()
    }),
    ("supports_dictionary_syntax", |dialect| {
        dialect.supports_dictionary_syntax()
    }),
    ("supports_window_clause_named_window_reference", |dialect| {
        dialect.supports_window_clause_named_window_reference()
    }),
    ("supports_parenthesized_set_variables", |dialect| {
        dialect.supports_parenthesized_set_variables()
    }),
    ("supports_select_wildcard_except", |dialect| {
        dialect.supports_select_wildcard_except()
    }),
    ("support_map_literal_syntax", |dialect| {
        dialect.support_map_literal_syntax()
    }),
    ("allow_extract_custom", |dialect| {
        dialect.allow_extract_custom()
    }),
    ("allow_extract_single_quotes", |dialect| {
        dialect.allow_extract_single_quotes()
    }),
    ("supports_extract_comma_syntax", |dialect| {
        dialect.supports_extract_comma_syntax()
    }),
    ("supports_create_view_comment_syntax", |dialect| {
        dialect.supports_create_view_comment_syntax()
    }),
    ("supports_parens_around_table_factor", |dialect| {
        dialect.supports_parens_around_table_factor()
    }),
    ("supports_values_as_table_factor", |dialect| {
        dialect.supports_values_as_table_factor()
    }),
    ("supports_create_index_with_clause", |dialect| {
        dialect.supports_create_index_with_clause()
    }),
    ("supports_explain_with_utility_options", |dialect| {
        dialect.supports_explain_with_utility_options()
    }),
    ("supports_limit_comma", |dialect| {
        dialect.supports_limit_comma()
    }),
    ("supports_update_order_by", |dialect| {
        dialect.supports_update_order_by()
    }),
    ("supports_projection_trailing_commas", |dialect| {
        dialect.supports_projection_trailing_commas()
    }),
    ("supports_asc_desc_in_column_definition", |dialect| {
        dialect.supports_asc_desc_in_column_definition()
    }),
    ("supports_try_convert", |dialect| {
        dialect.supports_try_convert()
    }),
    ("supports_bitwise_shift_operators", |dialect| {
        dialect.supports_bitwise_shift_operators()
    }),
    ("supports_comment_on", |dialect| {
        dialect.supports_comment_on()
    }),
    ("supports_load_extension", |dialect| {
        dialect.supports_load_extension()
    }),
    (
        "supports_named_fn_args_with_assignment_operator",
        |dialect| dialect.supports_named_fn_args_with_assignment_operator(),
    ),
    ("supports_struct_literal", |dialect| {
        dialect.supports_struct_literal()
    }),
    ("supports_empty_projections", |dialect| {
        dialect.supports_empty_projections()
    }),
    ("supports_nested_comments", |dialect| {
        dialect.supports_nested_comments()
    }),
    ("supports_multiline_comment_hints", |dialect| {
        dialect.supports_multiline_comment_hints()
    }),
    ("supports_user_host_grantee", |dialect| {
        dialect.supports_user_host_grantee()
    }),
    ("supports_string_escape_constant", |dialect| {
        dialect.supports_string_escape_constant()
    }),
    ("supports_array_typedef_with_brackets", |dialect| {
        dialect.supports_array_typedef_with_brackets()
    }),
    ("supports_match_against", |dialect| {
        dialect.supports_match_against()
    }),
    ("supports_set_names", |dialect| dialect.supports_set_names()),
    ("supports_comma_separated_set_assignments", |dialect| {
        dialect.supports_comma_separated_set_assignments()
    }),
    ("supports_filter_during_aggregation", |dialect| {
        dialect.supports_filter_during_aggregation()
    }),
    ("supports_select_wildcard_exclude", |dialect| {
        dialect.supports_select_wildcard_exclude()
    }),
    ("supports_data_type_signed_suffix", |dialect| {
        dialect.supports_data_type_signed_suffix()
    }),
    ("supports_interval_options", |dialect| {
        dialect.supports_interval_options()
    }),
    ("supports_quote_delimited_string", |dialect| {
        dialect.supports_quote_delimited_string()
    }),
    ("supports_select_wildcard_replace", |dialect| {
        dialect.supports_select_wildcard_replace()
    }),
    ("supports_select_wildcard_ilike", |dialect| {
        dialect.supports_select_wildcard_ilike()
    }),
    ("supports_select_wildcard_rename", |dialect| {
        dialect.supports_select_wildcard_rename()
    }),
    ("supports_optimize_table", |dialect| {
        dialect.supports_optimize_table()
    }),
    ("supports_install", |dialect| dialect.supports_install()),
    ("supports_detach", |dialect| dialect.supports_detach()),
    ("supports_prewhere", |dialect| dialect.supports_prewhere()),
    ("supports_with_fill", |dialect| dialect.supports_with_fill()),
    ("supports_limit_by", |dialect| dialect.supports_limit_by()),
    ("supports_interpolate", |dialect| {
        dialect.supports_interpolate()
    }),
    ("supports_settings", |dialect| dialect.supports_settings()),
    ("supports_select_format", |dialect| {
        dialect.supports_select_format()
    }),
    ("supports_comment_optimizer_hint", |dialect| {
        dialect.supports_comment_optimizer_hint()
    }),
    ("supports_constraint_keyword_without_name", |dialect| {
        dialect.supports_constraint_keyword_without_name()
    }),
    ("supports_key_column_option", |dialect| {
        dialect.supports_key_column_option()
    }),
    ("supports_comma_separated_trim", |dialect| {
        dialect.supports_comma_separated_trim()
    }),
    ("supports_cte_without_as", |dialect| {
        dialect.supports_cte_without_as()
    }),
    ("supports_select_item_multi_column_alias", |dialect| {
        dialect.supports_select_item_multi_column_alias()
    }),
    ("supports_xml_expressions", |dialect| {
        dialect.supports_xml_expressions()
    }),
];

#[test]
fn the_dialect_keeps_every_rule_of_the_generic_one() {
    let guarded = Guarded::reading(&[]);
    let (guarded, generic): (&dyn Dialect, &dyn Dialect) = (&guarded, &GenericDialect);
    for ch in ('\0'..='\u{3ff}').chain(['\u{4e2d}', '\u{1f600}']) {
        assert_eq!(
            guarded.is_delimited_identifier_start(ch),
            generic.is_delimited_identifier_start(ch),
            "is_delimited_identifier_start {ch:?}"
        );
        assert_eq!(
            guarded.is_identifier_start(ch),
            generic.is_identifier_start(ch),
            "is_identifier_start {ch:?}"
        );
        assert_eq!(
            guarded.is_identifier_part(ch),
            generic.is_identifier_part(ch),
            "is_identifier_part {ch:?}"
        );
    }
    for (rule, asked) in RULES {
        assert_eq!(asked(guarded), asked(generic), "{rule}");
    }
    assert_eq!(guarded.dialect(), generic.dialect());
}

/// What reading `text` spends and what its budget allows, read as [`Sql::parse`] reads it, and
/// whether the reading fails.
fn spending(text: &str) -> (usize, usize, bool) {
    let tokens = Tokenizer::new(&Guarded::reading(&[]), text)
        .tokenize_with_location()
        .unwrap();
    stack::text(text.len(), || {
        let dialect = Guarded::reading(&tokens);
        let parsed = Parser::new(&dialect)
            .with_recursion_limit(PARSER_DEPTH)
            .with_tokens_with_locations(tokens)
            .parse_statements();
        (dialect.spent.get(), dialect.reads, parsed.is_err())
    })
}

#[test]
fn a_query_read_with_its_table_first_is_refused() {
    assert!(GenericDialect.supports_from_first_select());
    assert!(!Guarded::reading(&[]).supports_from_first_select());
    let refused = Sql::parse("FROM input SELECT n").unwrap_err();
    assert_eq!(refused.kind(), SqlErrorKind::Syntax);
    // Read as a query and again as a join a level, 24 levels spent all their budget.
    let nest = "FROM(".repeat(24);
    let text = format!("SELECT {nest}1 +{} AS x FROM input", ")".repeat(24));
    let (spent, budget, refused) = spending(&text);
    assert!(refused, "{text}");
    assert!(spent < budget, "spent {spent} of {budget}");
}

#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "how long a parse takes is what this test holds, not a run's clock"
)]
fn text_failing_inside_a_nest_of_any_keyword_is_refused_without_reading_it_again() {
    // Read twice a level, 24 levels take sixteen million readings: seconds even optimised.
    let slow = Duration::from_millis(500);
    for word in ALL_KEYWORDS {
        for (open, close) in [("(", ")"), ("[", "]")] {
            let nest = format!("{word}{open}").repeat(24);
            let text = format!("SELECT {nest}1 +{} AS x FROM input", close.repeat(24));
            let started = Instant::now();
            assert!(Sql::parse(&text).is_err(), "{word}{open}");
            let took = started.elapsed();
            assert!(took < slow, "{word}{open} took {took:?}");
        }
    }
}

#[test]
fn a_keyword_is_still_read_as_a_name_where_its_own_syntax_nests_nothing() {
    assert!(Sql::parse("SELECT extract, match, position FROM input").is_ok());
    let refused = Sql::parse("SELECT cast FROM input").unwrap_err();
    assert_eq!(refused.kind(), SqlErrorKind::Syntax);
    assert!(Sql::parse("SELECT \"cast\" FROM input").is_ok());
    let called = Sql::parse("SELECT position(t, 'a') AS x FROM input").unwrap_err();
    assert_eq!(called.kind(), SqlErrorKind::Function);
}

/// Openers of nests the parser could read twice a level or more, each to be held open around the
/// next: types holding expressions, keywords holding expressions, and typed strings.
const OPENERS: [&str; 22] = [
    "STRUCT<a Nested(b INT DEFAULT ",
    "Tuple(Nested(a INT DEFAULT ",
    "Nullable(Nested(a INT DEFAULT ",
    "LowCardinality(Nested(a INT DEFAULT ",
    "MAP(INT, Nested(a INT DEFAULT ",
    "ARRAY<Nested(a INT DEFAULT ",
    "STRUCT<a INT OPTIONS(x = ",
    "Tuple(Nested(a INT MATERIALIZED ",
    "UNION(a Nested(b INT DEFAULT ",
    "TABLE(a ",
    "ARRAY<",
    "STRUCT<",
    "CAST(",
    "CASE WHEN ",
    "NOT (",
    "DEFAULT ",
    "MATERIALIZED ",
    "DATE '1' + ",
    "INT '1' + ",
    "POSITION(",
    "lower(",
    "(",
];

/// `text` refused, from a 2 MiB thread, within a second, however it nests.
#[expect(
    clippy::disallowed_methods,
    reason = "how long a parse takes is what this test holds, not a run's clock"
)]
fn refused_quickly(text: String) {
    let shown: String = text.chars().take(48).collect();
    let took = thread::Builder::new()
        .stack_size(2 << 20)
        .spawn(move || {
            let started = Instant::now();
            let refused = Sql::parse(&text).is_err();
            (refused, started.elapsed())
        })
        .unwrap()
        .join()
        .unwrap();
    assert!(took.0, "{shown}");
    assert!(took.1 < Duration::from_secs(1), "{shown} took {:?}", took.1);
}

/// `opened` repeated as often as `bytes` of text allow, broken at the innermost.
fn broken(opened: &str, bytes: usize) -> String {
    let levels = (bytes - 32) / opened.len();
    format!("SELECT {}1 + AS x FROM input", opened.repeat(levels))
}

#[test]
fn a_nest_of_types_holding_expressions_is_refused_where_it_opens() {
    for opener in &OPENERS[..10] {
        for bytes in [1 << 10, SQL_BYTES] {
            refused_quickly(broken(opener, bytes));
        }
    }
    let levels = (SQL_BYTES - 32) / 7;
    let array = format!(
        "SELECT {}INT{} AS x FROM input",
        "ARRAY<".repeat(levels),
        ">".repeat(levels)
    );
    refused_quickly(array);
}

#[test]
fn a_drawn_mix_of_openers_is_refused_quickly_at_the_text_s_limit() {
    // A fixed generator, so each run draws the same pairs and triples.
    let seed = Cell::new(0x9e37_79b9_7f4a_7c15_u64);
    let draw = |below: usize| {
        let mut next = seed.get();
        next ^= next << 13;
        next ^= next >> 7;
        next ^= next << 17;
        seed.set(next);
        usize::try_from(next % below as u64).unwrap()
    };
    for _ in 0..64 {
        let openers = 2 + draw(2);
        let opened: String = (0..openers).map(|_| OPENERS[draw(OPENERS.len())]).collect();
        refused_quickly(broken(&opened, SQL_BYTES));
    }
}

#[test]
fn a_nested_type_where_an_operand_stands_is_refused_for_its_shape() {
    for text in [
        "SELECT STRUCT(1, 2) AS x FROM input",
        "SELECT MAP(1, 2) AS x FROM input",
        "SELECT n FROM input WHERE ARRAY<INT> '[1]' IS NULL",
        "SELECT n FROM input WHERE map < 5",
    ] {
        let refused = Sql::parse(text).unwrap_err();
        assert_eq!(refused.kind(), SqlErrorKind::Shape, "{text}");
        assert!(refused.to_string().contains("a nested type"), "{refused}");
    }
    assert!(Sql::parse("SELECT map, tuple, nested, nullable FROM input").is_ok());
    assert!(Sql::parse("SELECT n FROM input WHERE \"map\" < 5").is_ok());
}

#[test]
fn a_parse_spending_more_than_its_budget_is_refused() {
    let text = format!("SELECT {}n", "n + ".repeat(31));
    let tokens = Tokenizer::new(&GenericDialect, &text)
        .tokenize_with_location()
        .unwrap();
    let parsed = |reads: usize, charges: Vec<usize>| {
        let dialect = Guarded {
            reads,
            spent: Cell::new(0),
            refused: Cell::new(None),
            charges,
        };
        let parsed = Parser::new(&dialect)
            .with_tokens_with_locations(tokens.clone())
            .parse_statements();
        (parsed.is_ok(), dialect.refused())
    };
    // Thirty-two reads, where a read costs one.
    assert_eq!(parsed(32, Vec::new()), (true, None));
    assert_eq!(parsed(31, Vec::new()), (false, Some(Refused::Work)));
    // Each `n +` costs three, as `n` may be a type and `+` its value, and the last `n` two.
    let charged = charges(&tokens);
    assert_eq!(parsed(95, charged.clone()), (true, None));
    assert_eq!(parsed(94, charged), (false, Some(Refused::Work)));
    assert_eq!(Guarded::reading(&tokens).reads, 64 * READS);
}

/// What a read at the start of `text` costs.
fn charged(text: &str) -> usize {
    let tokens = Tokenizer::new(&GenericDialect, text)
        .tokenize_with_location()
        .unwrap();
    charges(&tokens)[0]
}

#[test]
fn a_read_costs_every_token_its_typed_string_may_take() {
    assert_eq!(charged("1 + n"), 1);
    assert_eq!(charged("n + 1"), 3);
    assert_eq!(charged("a.b.c '1' + n"), 7);
    assert_eq!(charged("TIMESTAMP(3) WITH TIME ZONE '1' + n"), 9);
    assert_eq!(charged("INT[2][] '1' + n"), 8);
    assert_eq!(charged(&format!("f({}a) + n", "a, ".repeat(99))), 204);
    // A list within a call's is charged where it opens, not again where the call does.
    assert!(charged(&format!("f(g({}a)) + n", "a, ".repeat(99))) < 10);
}

#[test]
fn an_interval_where_an_operand_stands_is_refused_for_its_shape() {
    for text in [
        "SELECT INTERVAL '1' DAY AS x FROM input",
        "SELECT n FROM input WHERE t > t - INTERVAL '1 day'",
        "SELECT interval FROM input",
    ] {
        let refused = Sql::parse(text).unwrap_err();
        assert_eq!(refused.kind(), SqlErrorKind::Shape, "{text}");
        assert!(refused.to_string().contains("an interval"), "{refused}");
    }
    assert!(Sql::parse("SELECT \"interval\" FROM input").is_ok());
    let listed = ", a".repeat((SQL_BYTES - 256) / 3);
    let intervals = "INTERVAL ".repeat(20);
    refused_quickly(format!("SELECT {intervals}CAST(a{listed}) AS x FROM input"));
}

#[test]
fn a_run_of_any_keyword_at_the_text_s_limit_is_refused_quickly() {
    for word in ALL_KEYWORDS {
        refused_quickly(broken(&format!("{word} "), SQL_BYTES));
    }
}
