# Reports & Comments generating based on Cloud & GenAi

## Project Overview

This repository contains a project aimed at enhancing fully automated dashboards built on Google Cloud Platform (GCP) by integrating Generative AI (GenAI) models. The integration focuses on improving comment generation and trend analysis processes to boost efficiency and insights extraction.

## Data Description

The project involves handling extensive datasets related to the Europe Digital Transformation Program. Key performance indicators (KPIs) analyzed include E2E Digital Assisted sales, web funnels, E2E devices, and various other data sources spanning several years.

## Experiments

### Comments Generation Model - GPT-3.5

The project adapts GPT-3.5 for comments generation, moving away from the T5 model. Key differences include:

- **Training Approach**: GPT-3.5 Prmopt Engineering.
- **Flexibility**: GPT-3.5 offers versatile NLP capabilities without specific task training, unlike T5 which requires fine-tuning.
- **Context Understanding**: GPT-3.5’s autoregressive nature provides stronger context understanding and more coherent comments.

### GPT-3.5 Prompt Engineering

Effective prompt engineering with GPT-3.5 can generate insightful comments at minimal cost. Key steps include:

1. Define variables from the data table.
2. Craft a fixed prompt template with placeholders.

**Prompt Example**:
The [KPI_name] in [Country] is [increased/decreased/steady] with [KPI value current period] in the period of [current period] comparing with [KPI value previous period] in the period of [previous period].

**Output Example**:
The penetration rate in Poland is increased with 26.3% in YTD 2024 compared with 22.4% in the period of YTD 2023.


## Integration Pipeline

The pipeline for generating comments on the dashboard involves the following steps:

<img src="images/GPT pipeline.png" alt="Comments Generation Pipeline" width="800">

1. **Data Collection and Validation**: Raw data from multiple sources is collected and validated for quality.
2. **Data Storage in BigQuery**: Validated data is stored in Google BigQuery as structured tables.
3. **Cloud Functions**:
   - **First Function**: Handles automated data processing, performing calculations and creating a common table schema.
   - **Second Function**: Generates comments using Prompt Engineering and Azure OpenAI services. 
     - **Prompt Example**:
       ```python
       prompt_ = (f"For the month of {month} {year}, in {country}, the Year-to-Date (YTD)"
                 f"{column_name.replace('_', ' ')} is {current_value:.2f}%,"
                 f"compared to {previous_value:.2f}% in the same period last year."
                 "Generate a one sentence comment about the YoY performance metric.")
       ```
4. **Azure OpenAI Service**: Utilizes GPT-3.5-Turbo for generating comments via REST API. Example API call:
   ```python
   response = openai.ChatCompletion.create(
       engine = "AzureOpenAI deployment",
       messages= [
           {"role": "system", "content": "You will generate comments based on the given data."},
           {"role": "user", "content": prompt_}
       ],
       max_tokens = 50, temperature = 0.35
   )
