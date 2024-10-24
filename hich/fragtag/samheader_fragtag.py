from dataclasses import dataclass
import sys

# !Warning: this class has no specific unit test as of 2024/10/20 - Ben Skubi

@dataclass
class SamheaderFragtag:
    AT: str = "HC"
    ID: str = "hich_fragtag"
    PN: str = "hich_fragtag"
    CL: str = " ".join(sys.argv)
    PP: str = ""
    VN: str = "0.1"

    def __str__(self):
        return "\t".join([
            f"#samheader: @{self.AT} ID:{self.ID}",
            f"PN:{self.PN}",
            f"CL:{self.CL}",
            f"PP:{self.PP}",
            f"VN:{self.VN}"
        ]) + "\n"