"""The Expr layer decides whether generated SQL is valid Jinja at all."""

from generator.expr import Expr, compose, macro


def test_plain_sql_stays_plain():
    e = compose("count({0})", Expr.sql("event_id"))
    assert not e.is_jinja
    assert e.render() == "count(event_id)"


def test_macro_call_renders_as_a_single_jinja_block():
    e = macro("fs_array_size", Expr.sql("my_col"))
    assert e.render() == '{{ fs_array_size("my_col") }}'


def test_nested_macros_compose_into_one_expression_not_nested_braces():
    """The bug this type exists to prevent: {{ f("{{ g(...) }}") }} is invalid Jinja."""
    inner = macro("fs_array_union_agg", Expr.sql("p_col"))
    outer = macro("fs_array_size", inner)
    rendered = outer.render()
    assert rendered == '{{ fs_array_size(fs_array_union_agg("p_col")) }}'
    assert rendered.count("{{") == 1 and rendered.count("}}") == 1


def test_plain_sql_promotes_to_jinja_when_wrapping_a_macro():
    e = compose("coalesce({0}, 0)", macro("fs_array_size", Expr.sql("c")))
    assert e.is_jinja
    assert e.render() == '{{ "coalesce(" ~ (fs_array_size("c")) ~ ", 0)" }}'


def test_quotes_in_sql_are_escaped_into_the_jinja_literal():
    e = macro("fs_collect_set", Expr.sql("""case when x = 'a"b' then y end"""))
    assert '\\"' in e.render()


def test_braces_in_sql_are_not_treated_as_placeholders():
    e = compose("f({0})", Expr.sql("json_col->'{a}'"))
    assert e.render() == "f(json_col->'{a}')"
