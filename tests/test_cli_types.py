from hich.cli import *
from copy import deepcopy
from hypothesis import given, assume
from hypothesis import strategies as st
from pytest import raises
from typing import List, Union
from click import BadParameter

@st.composite
def random_case(draw, s: str) -> str:
    return "".join([draw(st.sampled_from([c.upper(), c.lower()])) for c in s])

@st.composite
def bool_list_params(draw):
    def to_params(L: List[bool]):
        def to_param(b: bool):
            if b:
                raw = draw(st.sampled_from(["true", "yes", "1"]))
            else:
                raw = draw(st.sampled_from(["false", "no", "0"]))
            return draw(random_case(raw))
        return [to_param(b) for b in L]

    target = draw(st.lists(st.booleans()))
    params = to_params(target)
    return (params, target)

@st.composite
def int_list_params(draw):
    target = draw(st.lists(st.integers()))
    params = [str(i) for i in target]
    return (params, target)

@st.composite
def str_list_params(draw):
    params = draw(st.lists(st.text()))
    return (params, None)


def separator():
    return st.sampled_from([" ", ",", "  "])

def do_strip():
    return st.sampled_from([True, False])

def chars_to_strip():
    return st.sampled_from([None, " ", "\t", " \t"])

@st.composite
def create_param_list(draw, list_type):
    separator = draw(get_separator())
    do_strip = draw(st.sampled_from([True, False]))
    chars_to_strip = draw(st.sampled_from([None, " ", "\t"]))

    return list_type(separator = separator, do_strip = do_strip, chars_to_strip = chars_to_strip)


def check_param_list(params, target, parser, user_separator, on_fatal_separator_mismatch, true_on_fatal_separator_mismatch = lambda a, b: True):
    value = user_separator.join(params)

    requires_separator = len(params) > 1
    compatible_separator_mismatch = (parser.separator == " " and user_separator == "  ")
    fatal_separator_mismatch = user_separator != parser.separator and requires_separator and not compatible_separator_mismatch

    if fatal_separator_mismatch and on_fatal_separator_mismatch:
        with raises(on_fatal_separator_mismatch):    
            converted = parser.convert(value, None, None)
            assert true_on_fatal_separator_mismatch(converted, target)
    else:
        converted = parser.convert(value, None, None)
        assert converted == target

@given(bool_list_params(), separator(), do_strip(), chars_to_strip(), separator())
def test_boolean_list(params_target, parser_separator, parser_do_strip, parser_chars_to_strip, user_separator):
    parser = _BooleanList(parser_separator, parser_do_strip, parser_chars_to_strip)
    check_param_list(*params_target, parser, user_separator, BadParameter)

@given(int_list_params(), separator(), do_strip(), chars_to_strip(), separator())
def test_int_list(params_target, parser_separator, parser_do_strip, parser_chars_to_strip, user_separator):
    parser = _IntList(parser_separator, parser_do_strip, parser_chars_to_strip)
    check_param_list(*params_target, parser, user_separator, BadParameter)

@given(str_list_params(), separator(), do_strip(), chars_to_strip(), separator())
def test_str_list(params_target, parser_separator, parser_do_strip, parser_chars_to_strip, user_separator):
    params, _ = params_target
    parser = _StrList(parser_separator, parser_do_strip, parser_chars_to_strip)
    value = user_separator.join(params)
    target = [t for t in value.split(parser.separator) if t]
    if parser.do_strip:
        target = [t.strip(parser.chars_to_strip) for t in target]
    params_target = (params, target)
    
    converted_length_1 = lambda converted, target: len(converted) == 1
    check_param_list(*params_target, parser, user_separator, None, converted_length_1)

