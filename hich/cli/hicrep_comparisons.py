import click
from hich.visuals import view_hicrep

@click.command(name='hicrep')
@click.option("--host", default = "127.0.0.1", show_default = True)
@click.option("--port", default = 8050, show_default = True)
def hicrep_comparisons(host, port):
    view_hicrep.run_dashboard(host, port)