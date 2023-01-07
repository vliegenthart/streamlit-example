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
# Parsel.ai Internal Analytics
This tool is a Work in Progress. For internal use only.
"""

items_table = Items()


def get_user():
    return items_table.query_user_by_email(st.session_state.email_input)


def epoch_to_human(epoch):
    return datetime.datetime.fromtimestamp(int(epoch) / 1000)


st.write("# User Metrics")

"""
Search for a user to display relevant user information.
"""


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


if not user:
    st.write("This user does not exist, please try searching again")
else:

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

    is_enterprise = user.get("gsi2Pk") and "enterprise" in user.get("gsi2Pk")
    st.write(f"**Is Enterprise Account**: {is_enterprise == True}")

    if is_enterprise:

        enterprise = items_table.query_enterprise(user.get("gsi2Pk"))
        enterprise_users = items_table.query_enterprise_users(user.get("gsi2Pk"))

        keys_to_extract_user = [
            "firstName",
            "lastName",
            "email",
            "datasetOptions",
            "createdAt",
            "lastSeenAt",
            "country",
            "monthlyQuota",
            "enterpriseQuota",
            "stripe",
            "updatedAt",
            "saveCard",
            "showNumberFormatBanner",
            "currentDocument",
        ]

        enterprise_users_to_display = [
            {key: item[key] for key in keys_to_extract_user if key in item}
            for item in enterprise_users
        ]

        st.write("#### Enterprise Account Users")

        enterprise_users_df = pd.DataFrame(enterprise_users_to_display)
        enterprise_users_df["createdAt"] = pd.to_datetime(
            enterprise_users_df["createdAt"], unit="ms"
        )

        enterprise_users_df["lastSeenAt"] = pd.to_datetime(
            enterprise_users_df["lastSeenAt"], unit="ms"
        )
        enterprise_users_df["updatedAt"] = pd.to_datetime(
            enterprise_users_df["updatedAt"], unit="ms"
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
            {key: item[key] for key in keys_to_extract if key in item}
            for item in datasets
        ]

        for dataset in datasets:
            if type(dataset.get("outputs")) == list:
                for output in dataset.get("outputs"):
                    if output.get("s3Key"):
                        dataset[
                            f"output_{output.get('format')}"
                        ] = create_presigned_url(output.get("s3Key"))

                dataset[f"input_document"] = create_presigned_url(dataset.get("key"))

                del dataset["outputs"]
                del dataset["key"]

        df = pd.json_normalize(datasets, max_level=1)
        df["createdAt"] = pd.to_datetime(df["createdAt"], unit="ms")
        df["updatedAt"] = pd.to_datetime(df["updatedAt"], unit="ms")

        st.write(df)
