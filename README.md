# Challenge General Information

You can read the details of the challenge at [challenge.md](challenge.md)

## Key Items

- __Due Date:__ 12/17/2025
- __Work Rules:__ You cannot work with others.  You can ask any question you want in our general channel. The teacher and TA are the only ones who can answer questions. __You cannot use code from other students' apps.__
- __Product:__ A streamlit app that runs within Docker, builds from your repo, and is published on Google Cloud Platform.
- __Github Process:__ Each student will fork the challenge repository and create their app. Their GitHub repo will have a link to the Cloud Run URL.
- __Canvas Process:__ Each student will upload a `.pdf` or `.html` file with their results as described in [challenge.md](challenge.md)
- Review the [Google Cloud Platform (GCP)](https://github.com/byuibigdata/google_cloud_platform) guide for setup instructions.

- EDIT: ALL completed requirements, hosted in gcp.


## Notes & References

- [Fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)
- [Creating a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)


## Details

- Please build dashboards that support a store owner in addressing the following questions (each question will have its own page or tab).

- Excluding fuels, what are the top five products with the highest weekly sales?
  
- In the packaged beverage category, which brands should I drop if I must drop some from the store?
  
- How do cash customers and credit customers compare?
  
- Which products are purchased most often for each customer type?
  
- How do the total purchase amounts compare?
  
- How does the total number of items compare?
  
- Provide the owners of the stores with detailed records a comparison of customer demographics within a specified area around their store using the Census API. Your demographic comparison needs to have at least 10 unique variables.


## Minimum Requirements

- All data must leverage Caching.
- Key Performance Indicators (KPIs) using st.metric() that address the question and provide context for comparisons. A clean summary table using Great Tables. Explore their examples to see the fantastic ideas for summary tables.
- At least two plotly or Altair graphs that help the user see temporal comparisons.
- Filters that limit the tables, charts, and KPIs to user-specified months and variable levels of interest in each question.
- Each question should leverage two unique items from the Layouts and Containers functionality.
- On at least one plot, allow the user to specify an input for a vertical or horizontal line that gets drawn on the chart. Create a use case that makes sense for your chart.



## Vocabulary/Lingo Challenge


Within your readme.md file in your repository and as a submitted .pdf or .html on Canvas, address the following items:

1. Explain the added value of using DataBricks in your Data Science process (using text, diagrams, and/or tables).

- The beauty of using a platform such as Databricks for Data Science processes is that the entire workflow is allocated to one domain instead of separate platforms, as one can perform everything in ETL, to cleansing practices, jobs/automation patterns, ML Training and even deploy straight from a cloud-based first infrastructure which just streamlines the entire workflows. In addition to this, Databricks is extremely fast in its overall performance, coupled with seamless version control and no on premise equipment and suddenly there is a clear reason as to why it's the go to service platform now. 

2. Compare and contrast PySpark to Pandas or the Tidyverse (using text, diagrams, and/or tables).

- Pandas & Tidyverse are ideal for smaller datasets for optimizing speed and performance, overall they are very intuitive to use with minimal issues for compatibility, but one limiting factor is that they are for localized usage only, whereas PySpark uses a cluster for compute (ie, cloudbased infrastructure that is distributed with way more ‘load power’ on-hand to use) alongside with being extremely fast with large datasets in particular as its just built for large-scale usage making it ideal for business applications. 

3. Explain Docker to somebody intelligent but not a tech person (using text, diagrams, and/or tables).

- When building Legos, you have all these different pieces in the box that need to fit together in a specific way to create the Lego Set. If you could package all those pieces together in a box with a specific instruction manual that ensures they fit perfectly every time you open it, no matter where you take it. Docker ensures that this happens no matter what. Docker creates a package for an application and all its dependencies to fit into a single container. Docker is the all-in-one platform for testing a program to ensure that it will work on anything that it is deployed on in the future; the instructions for Docker such as those used for Legos are hyper-specific to ensure that the end product is precisely the same, regardless of where or even who is building it.


4. Compare GCP to AWS for cost, features, and ease of use.

- From my research within this space:
- AWS vs Google Cloud (Cost):
  - 90 day, $300 of credit for new users on GCP
  - AWS has a free tier for 12 months; however, some services are always free on GCP but this was a weird nuance I found.
  - They both offer pay as you go, or steep discounts for reserved instances, which is a huge plus, but in terms of cost effectiveness, this would really just depend on the specific services you are using and how much you are using them. (ie, for both small scale projects, either would be sufficient, however, large scale projects on GCP seems to be easier to manage costs overall from my limited usage experience compared to AWS).
  
- AWS vs Google Cloud (Features):
    - AWS: AWS has features such as Lambda which assists with serverless computing, as well as other services such as S3 for storage, EC2 for computations, and it also supports a massive array of third-party integrations and other cloud services. It seems to be a more well-rounded service as its been around longer than GCP, overall though there is a steeper learning curve to it though which could offset some companies or users from using it though. 
    - GCP: It advertises itself with 'better' AI and ML services with enhanced data analytic tools as well as full-scale access to Kubernetes as a part of their service offering, but overall, GCP seems to be more streamlined towards data-centric applications and services at least for the time being. From my limited experience, it was way easier to use from a UI/UX perspective when compared to AWS, so the mileage may vary between the two services dependent on what you are willing to learn and use long-term. 
  
- AWS vs Google Cloud (Ease of Use):
    - Both of these services offer certifications as well as training guides and alot of user communities to help with setting up and troubleshooting, it seems like AWS has a much larger user base though due to its longer presence in the market. The UI for Google Cloud appears on the surface to be more intuitive and easier to navigate from my experience with the created projects/distribution that it offers, whereas AWS has a steeper learning curve as it can be difficult to manage all of the competing services but long-term seems to be more worthwhile as its got a track record of being the industry leader for uptime / reliability (ie, if we disregard the major outage from earlier this year). At the forefront, Google Cloud appears to be more user-friendly from the get-go, but AWS appears to be moreso streamlined towards handling large scale enterprise applications with more complex needs (ie, that differ and change, requiring alot of filtering/customization that currently Google Cloud doesn't offer as a part of its package).
  