## DATA &#8614; DASHBOARD

We learned [Streamlit](https://streamlit.io/) and [Docker](https://www.docker.com/) as two tools to use in the data science process. We will use these two tools to create an interactive app that allows users to explore the reliability of our data sources.

We will leverage our Cstore data for the Rigby and Ririe stores.

## Technologies for coding challenge

- __Required:__ [Streamlit](https://streamlit.io/) or [NiceGUI](https://nicegui.io/)
- __Required:__ [Docker](https://www.docker.com/)
- __Required:__ [Polars](https://pola-rs.github.io/polars/py-polars/html/index.html)
- __Required:__ [Cloud Run on GCP](https://cloud.google.com/run?hl=en)
- __Optional:__ [Plotly Express](https://plotly.com/python/plotly-express/)

## Coding Challenge

This coding challenge will take some time. Plan on regular work on this challenge throughout the remainder of the semester. If you start on this project a few days before it is due, you will most likely fail. You can use Google searches and AI to complete your work (please put commented links in your app with these references). You cannot use other humans besides the teacher and TA.

- __App Challenge:__ Build an App in a Docker Container and store the files in this repo so others can run your app.
- __Vocabulary Challenge:__ Review the text below and answer the questions.

Please read below for details on how to complete each challenge area.

### App Challenge

#### Data Description

You have detailed data for five stores in Idaho. You have all the convenience store locations in Idaho.

#### Details

Please build dashboards that support a store owner in addressing the following questions (each question will have its own page or tab).

1. Excluding fuels, what are the top five products with the highest weekly sales?
2. In the packaged beverage category, which brands should I drop if I must drop some from the store?
3. How do cash customers and credit customers compare?
  - Which products are purchased most often for each customer type?
  - How do the total purchase amounts compare?
  - How does the total number of items compare?
4. Provide the owners of the stores with detailed records a comparison of customer demographics within a specified area around their store using the Census API. Your demographic comparison needs to have at least 10 unique variables.

At a minimum, your application should include the following elements for each of the three key questions (if using NiceGUI, you must locate the corresponding functionality).

1. All data must leverage [Caching](https://docs.streamlit.io/develop/api-reference/caching-and-state).
2. [Key Performance Indicators (KPIs)](https://docs.streamlit.io/develop/api-reference/data/st.metric) using `st.metric()` that address the question and provide context for comparisons.
A clean summary table using [Great Tables](https://posit-dev.github.io/great-tables/articles/intro.html). Explore [their examples](https://posit-dev.github.io/great-tables/examples/) to see the fantastic ideas for summary tables.
3. At least two plotly or Altair graphs that help the user see temporal comparisons.
4. Filters that limit the tables, charts, and KPIs to user-specified months and variable levels of interest in each question. 
5. Each question should leverage two unique items from the [Layouts and Containers](https://docs.streamlit.io/develop/api-reference/layout) functionality.
6. On at least one plot, allow the user to specify an input for a vertical or horizontal line that gets drawn on the chart. Create a use case that makes sense for your chart.

#### Bonus Challenge

- Get your app to work with an external database instead of having the data stored in the repository. Completing this part can count for 9 hours or raise one of your challenge scores by one point.

#### Data Science Dashboard

We will use Streamlit as our prototype dashboard tool, but we need to embed that streamlit app into a Docker container.

Within this repository, you can simply run `docker compose up` to leverage the `docker-compose.yaml` with your local folder synced with the container folder where the streamlit app is running. 

### Repo Structure

Your repo should be built so that I can clone the repo and run the Docker command (`docker compose up`) as described in your `readme.md`. This will allow me to see your app in my web browser without requiring me to install Streamlit on my computer.

1. Fork this repo to your private space.
2. Add me to your private repo in your space (`hathawayj`)
3. Build your app using your Docker container
4. Update your `readme.md` with details about your app.
5. Include a link in your `readme.md` to your GitHub repository and the URL for our app on Cloud Run.

### Vocabulary/Lingo Challenge

_Within your `readme.md` file in your repository and as a submitted `.pdf` or `.html` on Canvas, address the following items:_

1. Explain the added value of using DataBricks in your Data Science process (using text, diagrams, and/or tables).
2. Compare and contrast PySpark to Pandas or the Tidyverse (using text, diagrams, and/or tables).
3. Explain Docker to somebody intelligent but not a tech person (using text, diagrams, and/or tables).
4. Compare GCP to AWS for cost, features, and ease of use.

_Your answers should be clear, detailed, and no longer than is needed. Imagine you are responding to a client or as an interview candidate._

- _Clear:_ Clean sentences and nicely laid out format.
- _Detailed:_ You touch on all the critical points of the concept. Don't speak at too high a level.
- _Brevity:_ Don't ramble. Get to the point, and don't repeat yourself.

