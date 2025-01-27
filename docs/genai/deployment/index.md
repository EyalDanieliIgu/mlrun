(genai-deployment)=
# Deploying gen AI application serving pipelines

MLRun serving can produce managed ML application pipelines using real-time auto-scaling Nuclio serverless functions. 
The application pipeline includes all the steps including: accepting events or data, preparing the required model features, 
inferring results using one or more models, and driving actions.

**In this section**

```{toctree}
:maxdepth: 1

genai_serving
gpu_utilization
genai_serving_graph
```

**See also**
- {ref}`genai_01_basic_tutorial`
- {ref}`genai-02-mm-llm`
- {ref}`realtime-monitor-drift-tutor`
- {ref}`model-monitoring-overview`
- {ref}`alerts-notifications`