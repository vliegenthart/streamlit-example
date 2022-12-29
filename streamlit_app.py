from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
from dynamodb_class import Items, DecimalEncoder
import json

"""
# Welcome to Streamlit!

Edit `/streamlit_app.py` to customize this app to your heart's desire :heart:

If you have any questions, checkout our [documentation](https://docs.streamlit.io) and [community
forums](https://discuss.streamlit.io).

In the meantime, below is an example of what you can do with just a few lines of code:
"""

"""
# TODO
- Dynamodb pages per month, and per top users
- Search for Dynamodb Enterprise user specific usage
"""

items_table = Items()


def get_user():
    user = items_table.query_user_by_email(st.session_state.email_input)
    return json.loads(json.dumps(user, indent=4, cls=DecimalEncoder))


INIT_VALUE = "daniel@parsel.ai"

user_email = st.text_input(
    "User email",
    value=INIT_VALUE,
    max_chars=None,
    key="email_input",
    type="default",
    help=None,
    autocomplete=None,
    on_change=get_user,
    args=None,
    kwargs=None,
    placeholder=None,
    disabled=False,
    label_visibility="visible",
)

output = get_user()
st.write(output)


def lets_write_yo(data):
    st.write(data)


# with st.echo(code_location="below"):
#     total_points = st.slider("Number of points in spiral", 1, 5000, 2000)
#     num_turns = st.slider("Number of turns in spiral", 1, 100, 9)

#     Point = namedtuple("Point", "x y")
#     data = []

#     points_per_turn = total_points / num_turns

#     for curr_point_num in range(total_points):
#         curr_turn, i = divmod(curr_point_num, points_per_turn)
#         angle = (curr_turn + 1) * 2 * math.pi * i / points_per_turn
#         radius = curr_point_num / total_points
#         x = radius * math.cos(angle)
#         y = radius * math.sin(angle)
#         data.append(Point(x, y))

#     st.altair_chart(
#         alt.Chart(pd.DataFrame(data), height=500, width=500)
#         .mark_circle(color="#0068c9", opacity=0.5)
#         .encode(x="x:Q", y="y:Q")
#     )
