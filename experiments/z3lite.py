from z3 import *
import sqlite3
import operator

z3_ast_table = {}


def get_id(x: AstRef):
    id_ = x.get_id()
    z3_ast_table[id_] = x
    return id_


def lookup_id(id_: bytes):
    return z3_ast_table[int(id_)]


sqlite3.register_adapter(AstRef, get_id)
sqlite3.register_adapter(BoolRef, get_id)
sqlite3.register_adapter(ArithRef, get_id)
sqlite3.register_adapter(BitVecRef, get_id)
sqlite3.register_adapter(CharRef, get_id)
sqlite3.register_adapter(DatatypeRef, get_id)

sqlite3.register_adapter(BitVecNumRef, get_id)
sqlite3.register_adapter(RatNumRef, get_id)
sqlite3.register_adapter(IntNumRef, get_id)
sqlite3.register_adapter(AlgebraicNumRef, get_id)

sqlite3.register_converter("AstRef", lookup_id)
sqlite3.register_converter("BoolRef", lookup_id)
sqlite3.register_converter("ArithRef", lookup_id)
sqlite3.register_converter("BitVecRef", lookup_id)
sqlite3.register_converter("CharRef", lookup_id)
sqlite3.register_converter("DatatypeRef", lookup_id)


def check_sat(e: bytes):
    s = Solver()
    s.add(lookup_id(e))
    res = s.check()
    return repr(res)


def enable_z3(con):
    def create_z3_2(name, f):
        def wrapf(x, y):
            return get_id(simplify(f(lookup_id(x), lookup_id(y))))
        con.create_function(name, 2, wrapf, deterministic=True)

    def create_z3_1(name, f):
        def wrapf(x):
            return get_id(simplify(f(lookup_id(x))))
        con.create_function(name, 1, wrapf, deterministic=True)
    # I could possibly do this as an .so sqlite extension instead.
    create_z3_2("z3_and", And)
    create_z3_2("z3_or", Or)
    create_z3_2("z3_implies", Implies)
    create_z3_2("z3_eq", operator.eq)
    create_z3_2("z3_le", operator.le)
    create_z3_2("z3_lt", operator.lt)
    create_z3_2("z3_ge", operator.ge)
    create_z3_2("z3_gt", operator.gt)
    create_z3_2("z3_ne", operator.ne)
    create_z3_2("z3_add", operator.add)
    create_z3_2("z3_mul", operator.add)
    create_z3_2("z3_sub", operator.sub)
    create_z3_2("z3_bvand", operator.__and__)
    create_z3_2("z3_bvor", operator.__or__)
    create_z3_2("z3_rshift", operator.__rshift__)
    create_z3_2("z3_lshift", operator.__lshift__)

    create_z3_1("z3_neg", operator.neg)
    create_z3_1("z3_not", Not)

    con.create_function("check_sat", 1, check_sat, deterministic=True)
    # con.create_function("z3_and", 2, lambda x, y:
    #                    get_id(simplify(And(lookup_id(x), lookup_id(y)))), deterministic=True)
