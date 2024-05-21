import streamlit as st
import pandas as pd
import networkx as nx
import plotly.graph_objects as go
from dotenv import load_dotenv
load_dotenv()
from Utils.proccess_config import get_snowflake_conn
from Utils.utils import get_completion
st.set_page_config(layout="wide")



@st.cache_data
def load():
    ctx = get_snowflake_conn()
    df = pd.read_sql('''SELECT "from","to" FROM EDGAR_COMPETITORS_GRAPH''',ctx)
    return df


def create_traces(
    G,
    pos,
    color_scheme,
    show_labels,
    node_size,
    edge_thickness,
    show_labels_on_hover,
    edge_color,
    node_shape,
):
    edge_x, edge_y, node_x, node_y, node_degrees, text = [], [], [], [], [], []

    for edge in G.edges():
        x0, y0, x1, y1 = *pos[edge[0]], *pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    hover_text = (
        [f"{node} ({G.nodes[node].get('label', 'NA')})" for node in G.nodes()]
        if show_labels_on_hover
        else ["" for _ in G.nodes()]
    )

    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_degrees.append(G.degree(node))
        if show_labels:
            text.append(f"{node} ({G.nodes[node].get('label', 'NA')})")
        else:
            text.append("")

    actual_edge_color = edge_color if edge_color else "#888"

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=edge_thickness, color=actual_edge_color),
        hoverinfo="none",
        mode="lines",
    )

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers+text" if show_labels else "markers",
        hoverinfo="text" if show_labels_on_hover else "none",
        text=hover_text if show_labels_on_hover else text,
        marker=dict(
            symbol=node_shape,
            showscale=True,
            colorscale=color_scheme,
            size=node_size,
            color=node_degrees,
            colorbar=dict(
                thickness=15,
                title="Node Connections",
                xanchor="left",
                titleside="right",
            ),
            line_width=2,
        ),
    )

    return edge_trace, node_trace


def plot_graph(
    G,
    layout_spacing,
    color_scheme,
    show_labels,
    node_size,
    edge_thickness,
    show_labels_on_hover,
    edge_color,
    node_shape,
):
    pos = nx.spring_layout(G, k=layout_spacing)
    edge_trace, node_trace = create_traces(
        G,
        pos,
        color_scheme,
        show_labels,
        node_size,
        edge_thickness,
        show_labels_on_hover,
        edge_color,
        node_shape,
    )

    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title="Knowledge Graph Visualization",
            titlefont_size=20,
            showlegend=False,
            hovermode="closest",
            margin=dict(b=20, l=5, r=5, t=40),
            annotations=[
                dict(
                    text="Constructed from Edgar Filings",
                    showarrow=False,
                    xref="paper",
                    yref="paper",
                    x=0.005,
                    y=-0.002,
                )
            ],
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor="white",
        ),
    )

    return fig

df_links = load()
G = nx.Graph()
for link in df_links.index:
  G.add_edge(df_links.iloc[link]['from'],
             df_links.iloc[link]['to'])
# nx.draw(G, with_labels=True)

st.title('Insight Engine')
st.caption('Please enter a news article below. Then select a main entity.')

with st.container():
    news_article_input = st.text_area('News Article',height=300)

    uniq_orgs = sorted(list(set(df_links['from'].tolist())))
    src_node = st.selectbox('Main Entity',uniq_orgs,0)
    cutoff_int = st.number_input('Depth Limit',min_value=1,max_value=None,value=3)

    deps_list = list(nx.dfs_edges(
        G, 
        src_node,
        depth_limit=cutoff_int,
        ))

    layout_spacing = 0.5
    color_scheme = "ylgnbu"
    show_labels = True
    show_labels_on_hover = False
    node_size = 10
    edge_thickness = 0.5
    node_shape = "circle"
    edge_color = "#888"

    subset_nodes = []
    for x in deps_list:
        for x2 in x:
            subset_nodes.append(x2)
    subset_nodes_uniq = sorted(list(set(subset_nodes)))
    K = G.subgraph(subset_nodes_uniq)

    fig = plot_graph(
        K,
        layout_spacing,
        color_scheme,
        show_labels,
        node_size,
        edge_thickness,
        show_labels_on_hover,
        edge_color,
        node_shape,
    )

    plotly_chart_config = {
        "displayModeBar": True,
        "displaylogo": False,
        "modeBarButtonsToRemove": [
            "zoomIn2d",
            "zoomOut2d",
            "autoScale2d",
            "resetScale2d",
        ],
        "modeBarPosition": "bottom",
        "toImageButtonOptions": {
            "format": "png",
            "filename": "knowledge_graph",
            "height": 1080,
            "width": 2048,
            "scale": 2,
        },
    }

    st.plotly_chart(
        fig, use_container_width=True, config=plotly_chart_config
    )

    

    main_entity = src_node
    text = news_article_input
    competitors = deps_list[:1000]

    user_prompt = f"""You are a helpful financial assistant. Your task is to help a portfolio manager assess the impact of a news article about about entity in the manager's portfolio of equity holdings. In addition, you will be provided context from a knowledge graph of competitors, partners, and suppliers to the main entity. 
    Here is the news article.
    <article>
    {text}
    </article>

    Here are the competitors from the knowledge graph. The competitors are shown as edges from the main entity. The output was produced using nx.dfs_edges(G, src_node, depth_limit=cutoff_int))
    <edges>
    {competitors}
    </edges>

    Based on the context above, perform the following task: hypothesize a cause and effect chain that relates the news article to the dependencies in the graph. Specifically, propose a 2 step sequence of events, that starts with the context present in the news article, and extends 6 -8 months into the future based on the dependencies in the graph. Specifically focus on indirect dependencies that would be invisible to other market participants who only have access to the news article but not the graph.
    """

    if news_article_input and src_node:
        res = get_completion(user_prompt, 'meta-llama/Llama-3-8b-chat-hf')
        st.markdown('## Investment Analyst')
        st.write(res)

        st.divider()
        prompt2 = f'From the perspective of a data scientist, use the market map represented as an undirected graph (nodes and edges), to assess the strengths and weaknesses fo the following investment hypothesis: ```{res}``` Market Map Edges: ```{competitors}```'
        
        res2 = get_completion(prompt2,'Snowflake/snowflake-arctic-instruct')
        st.markdown('## Data Scientists ')
        st.write(res2)


    