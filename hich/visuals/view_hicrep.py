import seaborn as sns
import time
import dash
from dash import dcc, html, callback, Output, Input, State, ALL, callback_context
import base64
import io
import polars as pl
from natsort import natsorted
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt
import time
import webbrowser
from threading import Timer

def seaborn_clustermap(symmetric_df, settings = {}):
    cell_size = .6
    cell_count = len(symmetric_df)
    size = min(16, cell_size*cell_count)
    figsize=(size, size)
    cell_size = size / cell_count
    sns.set(font_scale=cell_size/.6)  # Seaborn uses a relative scaling factor
    
    g = sns.clustermap(symmetric_df,
                        annot=True,
                        fmt=".2f",
                        cmap="viridis",
                        cbar=False,
                        dendrogram_ratio=(.05, .05),
                        figsize = figsize)
    g.ax_heatmap.set_facecolor("gray")
    g.cax.set_visible(False)
    
    return g

def scc_with_distance(data):
    # Compute distance metric from SCC score
    scc = data["scc"]
    dist = ((.5*(1 - scc))**.5).alias("score")
    
    # Rename SCC column to "score"
    scc_data = data.rename({"scc":"score"})
    dist_data = scc_data.clone()
    
    # Add distance in "score" column for dist_data
    dist_data = dist_data.with_columns(dist)

    # Create label for type of score
    scc_label = pl.Series(["scc"]*len(scc)).alias("cluster by")
    dist_label = pl.Series(["distance: 0 ≤ √[.5(1-scc)] ≤ 1"]*len(dist)).alias("cluster by")

    # Add score type label to column
    scc_data = scc_data.with_columns(scc_label)
    dist_data = dist_data.with_columns(dist_label)

    # Concatenate SCC and distance dataframes
    return pl.concat([scc_data, dist_data])


app = dash.Dash(__name__)

model = None
states = None
filename = ""
state = {}
dummy = {"__dummy__":[]}
options = {"__dummy__":[]}
multi_options = {"__dummy__":[]}
static_options = {"__dummy__":[]}
data_cols = ["file1", "file2", "score"]
cluster_data = None

# Placeholder for storing dataset options
app.layout = html.Div([
    dcc.Upload(id='upload-data', children=html.Button('Upload Data')),
    dcc.Store(id='__dummy__'),
    dcc.Store(id='multi-options'),
    dcc.Store(id='static-options'),
    html.Div(
        id = "clustermap-image",
        style={
            'display': 'flex',
            'justify-content': 'center',
            'align-items': 'center',
            'width': '100%'  # Ensure the container takes up full width
        }
    ),
    html.Div(id='static-options-container'),
    html.Div(id='slider-container')  # This will hold the sliders
])

@callback(
    [Output('multi-options', 'data'),
     Output('static-options', 'data')],
    Input('upload-data', 'contents'),
    State('upload-data', 'filename')
)
def load_data(contents, upload_filename):
    global model, data_cols, options, states, multi_options, static_options, dummy, filename
    if contents is None:
        return {}, {}

    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    filename = upload_filename
    
    # Use io.BytesIO to create a file-like object from the decoded content
    uploaded = pl.read_csv(io.BytesIO(decoded), separator="\t")
    model = scc_with_distance(uploaded)

    options = {col: natsorted(model[col].unique())
               for col in model.columns
               if col not in data_cols}
    
    states = model.select(options.keys()).unique()
    state = states.row(0, named = True)
    
    multi_options = {col: options[col]
                     for col in options
                     if len(options[col]) > 1}
    static_options = {"filename": [filename]}
    static_options.update({col: options[col]
                     for col in options
                     if len(options[col]) == 1})

    multi_options.update(dummy)
    static_options.update(dummy)

    return multi_options, static_options

@callback(
    Output('slider-container', 'children'),
    Input('multi-options', 'data')
)
def update_multi_options(new_multi_options):
    global model, data_cols, options, states, multi_options, static_options, dummy
    if new_multi_options: new_multi_options.pop('__dummy__')
    if not new_multi_options:
        return []

    slider_elements = []
    for option_name, option_settings in new_multi_options.items():
        char_count = sum([len(str(setting)) for setting in option_settings])
        width = char_count * 9
        slider_elements.append(
            html.Div([
                dcc.Slider(id = {'type': 'dynamic-slider', 'index': option_name},
                           min = 0,
                           max = len(option_settings)-1,
                           step=None,
                           value = 0,
                           marks = {str(i): str(setting) for i, setting in enumerate(option_settings)})
            ], style={"width": f"{width}px", 'margin': '10px 0', 'justify-content': 'center'})
        )
    return html.Div(slider_elements, style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center', 'width': '100%'})


@callback(
    Output('static-options-container', 'children'),
    Input('static-options', 'data')
)
def update_static_options(new_static_options):
    global model, data_cols, options, states, multi_options, static_options, dummy
    if new_static_options: new_static_options.pop('__dummy__')
    if not new_static_options:
        return []

    p_elements = []
    for option_name, option_settings in new_static_options.items():
        p_elements.append(
            html.Div([html.P(f"{option_name}: {option_settings[0]}")])
        )

    return html.Div(p_elements, style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center', 'width': '100%'})

@app.callback(
    Output('clustermap-image', 'children'),
    [Input({'type': 'dynamic-slider', 'index': ALL}, 'value')]
)
def update_output(*slider_values):
    global model, data_cols, options, states, multi_options, static_options, dummy
    # Access callback context to get both IDs and values
    ctx = callback_context
    if not ctx.triggered or model.is_empty():
        return html.Div()

    # Extract the IDs and their corresponding values
    triggered_inputs = ctx.inputs_list[0]
    update = {}
    for trigger in triggered_inputs:
        option = trigger['id']['index']
        setting_idx = trigger['value']
        setting = options[option][setting_idx]
        update[option] = setting
    state.update(update)

    cluster_data = model.join(pl.DataFrame(state), on = state.keys()).select(data_cols)

    fig = clustermap_figure(cluster_data)
    return dcc.Graph(figure=fig)

def clustermap_figure(data):
    
    df = data.to_pandas()

    # Step 1: Pivot the DataFrame
    pivot_df = df.pivot(index='file1', columns='file2', values='score')
    
    # Step 2: Make the pivot symmetric by copying transposed values
    # Fill missing values in the pivot by using values from its transpose
    symmetric_df = pivot_df.combine_first(pivot_df.T)

    
    setting_labels = [f"filename: {filename}"] + [f"{option}: {setting}" for option, setting in state.items()]
    title = "HicRep Clustermap: " + ", ".join(setting_labels)
    settings = {
        "title": title
    }

    clustermap = seaborn_clustermap(symmetric_df, settings)

    # Save the clustermap as an image in memory
    buf = io.BytesIO()
    clustermap.savefig(buf, format="png")
    plt.close(clustermap.fig)
    buf.seek(0)
    

    # Encode the image in base64
    encoded_image = base64.b64encode(buf.getvalue()).decode()
    # Get the image dimensions for proper sizing
    width, height = clustermap.fig.get_size_inches() * clustermap.fig.dpi

    # Create and return the figure containing the updated image
    fig = go.Figure()

    fig.add_layout_image(
        dict(
            source=f"data:image/png;base64,{encoded_image}",
            xref="x",
            yref="y",
            x=0,
            y=height,
            sizex=width,
            sizey=height,
            sizing="stretch",
            opacity=1,
            layer="below"
        )
    )

    # Hide axes, gridlines, etc.
    fig.update_xaxes(visible=False, range=[0, width])
    fig.update_yaxes(visible=False, range=[0, height])
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        xaxis=dict(showgrid=False, zeroline=False, fixedrange=True),
        yaxis=dict(showgrid=False, zeroline=False, fixedrange=True),
        width=width,
        height=height
    )
    

    return fig

def make_demo_file():
    from itertools import combinations, combinations_with_replacement, product, chain
    from random import random
    from collections import defaultdict
    files = [f"file{i}" for i in range(50)]
    chroms = [f"chr{i}" for i in range(22)] + ["chrX", "chrY", "chrM"]
    h = ["1"]
    dBPMax = [None]
    resolutions = [100000, 1000000]
    file_combos = combinations_with_replacement(files, 2)
    param_combos = product(chroms, resolutions, h, dBPMax)
    rows = [files + params for files, params in product(file_combos, param_combos)]
    columns = ["file1", "file2", "chrom", "resolution", "h", "dBPMax", "scc"]
    settings = defaultdict(list)
    for row in rows:
        for val, column in zip(row, columns):
            settings[column].append(val)
        settings["scc"].append(random())
    settings = pl.DataFrame(settings)
    settings.write_csv("demo.tsv", separator = "\t")

def open_browser(host, port):
    url = f"http://{host}:{port}/"
    webbrowser.open_new(url)

def run_dashboard(host, port):
    Timer(1, open_browser, args=(host, port)).start()
    app.run_server(host = host, port = port, debug=True, use_reloader=False)

if __name__ == '__main__':
    run_dashboard()

