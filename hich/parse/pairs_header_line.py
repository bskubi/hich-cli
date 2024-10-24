from hich.parse.pairs_line import PairsLine
from pydantic import field_validator, constr, BaseModel

class PairsHeaderLine(BaseModel):
    line: constr(pattern=r'^#.*')


