import faker
import uuid
import time
from snowflake import SnowflakeGenerator

fake = faker.Faker()
sf_gen = SnowflakeGenerator(42)

class ExprRuntime:
    func_map = {
        "name": lambda: fake.name(),
        "phone": lambda: fake.phone_number(),
        "address": lambda: fake.address(),
        "now": lambda: time.strftime("%Y-%m-%d %H:%M:%S"),
        "uuid": lambda: str(uuid.uuid4()),
        "snowflake": lambda: next(sf_gen),
    }

    @classmethod
    def eval(cls, rule: str, ctx: dict):
        """
        rule: 表达式字符串，如 name() / ${node01.user_id_list}
        ctx: DAG上下文变量字典
        """
        if not rule:
            return None
        # 变量引用解析 ${xxx}
        if rule.startswith("${") and rule.endswith("}"):
            var_key = rule[2:-1].strip()
            return ctx.get(var_key, None)
        # 内置函数调用
        if rule.endswith("()"):
            fn_name = rule[:-2]
            if fn_name in cls.func_map:
                return cls.func_map[fn_name]()
        # 常量直接返回
        return rule
