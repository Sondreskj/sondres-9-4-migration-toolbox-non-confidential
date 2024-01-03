# Copyright (c) Streamlit Inc. (2018-2022) Snowflake Inc. (2022)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import streamlit as st
from streamlit.logger import get_logger

LOGGER = get_logger(__name__)


def run():
    st.set_page_config(
        page_title="Sondres lekeplass",
        page_icon="👋",
    )

    st.write("# Welcome to migration shortcut world! 👋")

    st.sidebar.success("Select a beautiful program above.")

    st.markdown(
        """
      In the menu to the left a bunch of regex scrips are created in order to make the refactoring easier. 
    """
    )
    with st.columns(3)[1]:
        st.markdown("![Alt Text](https://media.tenor.com/vyrfV9Lp5AwAAAAM/refactor-del-siglo-winter-is-coming.gif)")
if __name__ == "__main__":
    run()
