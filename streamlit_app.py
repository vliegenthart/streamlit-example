from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
from dynamodb_class import Items, DecimalEncoder, create_presigned_url
import json
import datetime
from benedict import benedict

"""
# Welcome to Streamlit!

Edit `/streamlit_app.py` to customize this app to your heart's desire :heart:

If you have any questions, checkout our [documentation](https://docs.streamlit.io) and [community
forums](https://discuss.streamlit.io).

In the meantime, below is an example of what you can do with just a few lines of code:
"""

"""
# TODO
- ADD DYNAMODB CREDENTIALS TO CLOUD VERSION
- Dynamodb pages per month, and per top users
- Search for Dynamodb Enterprise user specific usage
"""

items_table = Items()


def get_user():
    return items_table.query_user_by_email(st.session_state.email_input)


def epoch_to_human(epoch):
    return datetime.datetime.fromtimestamp(int(epoch) / 1000)


st.write("# User Metrics")

INIT_VALUE = "daniel@parsel.ai"

user_email = st.text_input(
    "User email",
    value=INIT_VALUE,
    max_chars=None,
    key="email_input",
    type="default",
    help=None,
    autocomplete=None,
    on_change=None,
    args=None,
    kwargs=None,
    placeholder=None,
    disabled=False,
    label_visibility="visible",
)


user = get_user()


st.checkbox(
    "Show Detailed User Details",
    value=False,
    key="show_user",
    help=None,
    on_change=None,
    disabled=False,
    label_visibility="visible",
)

if st.session_state.show_user:
    st.write(user)

# st.write(user)

is_enterprise = "enterprise" in user.get("gsi2Pk")
st.write(f"**Is Enterprise Account**: {is_enterprise}")

if is_enterprise:

    enterprise = items_table.query_enterprise(user.get("gsi2Pk"))
    enterprise_users = items_table.query_enterprise_users(user.get("gsi2Pk"))

    # st.write(enterprise["trialQuota"], type(enterprise))
    # st.write(enterprise_users)
    st.write("#### Enterprise Users")
    st.write("FIXMYATTRIBUTES + TEST WHAT HAPPENS WITH DIFFERENT USER INPUT")
    enterprise_users_df = pd.DataFrame(enterprise_users)
    enterprise_users_df["createdAt"] = pd.to_datetime(
        enterprise_users_df["createdAt"], unit="ms"
    )
    st.write(enterprise_users_df)

    is_on_trial = enterprise.get("trialQuota") is not None

    st.write(f"**Is On Trial**: {is_on_trial}")

    if is_on_trial:
        st.write(
            f'**Trial started at**: {epoch_to_human(enterprise["trialQuota.createdAt"])}'
        )

        st.write(
            f'**Trial Ends at**: {epoch_to_human(enterprise["trialQuota.endsAt"])}'
        )
        st.write(
            f'**Page usage**: {enterprise["trialQuota.pagesConsumed"]}/{enterprise["trialQuota.pagesAllowed"]}'
        )


st.write("#### All Enterprise Account Datasets")
st.write("If empty, loading...")


datasets = []
if is_enterprise:
    for user in enterprise_users:
        datasets = datasets + items_table.query_user_datasets(user.get("pk"))
else:
    datasets = items_table.query_user_datasets(user.get("pk"))


keys_to_extract = [
    "fileName",
    "status",
    "outputs",
    "audit",
    "name",
    "createdAt",
    "fileSize",
    "deleted",
    "pageCount",
    "numberFormat",
    "updatedAt",
    "progress",
    "key",
]

datasets = [
    {key: item[key] for key in keys_to_extract if key in item} for item in datasets
]


for dataset in datasets:
    if type(dataset.get("outputs")) == list:
        for output in dataset.get("outputs"):
            if output.get("s3Key"):
                dataset[f"output_{output.get('format')}"] = create_presigned_url(
                    output.get("s3Key")
                )

        dataset[f"input_document"] = create_presigned_url(dataset.get("key"))

        del dataset["outputs"]
        del dataset["key"]

df = pd.json_normalize(datasets, max_level=1)
df["createdAt"] = pd.to_datetime(df["createdAt"], unit="ms")

st.write(enterprise_users_df)

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
