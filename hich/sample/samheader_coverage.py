from dataclasses import dataclass
import sys

@dataclass
class SamheaderCoverage:
    AT: str = "HC"
    ID: str = "hich_coverage"
    PN: str = "hich_coverage"
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