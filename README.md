# Auto Trader #

## Overview ##

Auto Trader is an end-to-end machine learning pipeline designed for automated 
cryptocurrency trading. The pipeline leverages powerful technologies such as 
Python, AWS, MongoDB, Scikit-learn, PyTorch, XGBoost, and MLflow. It is 
meticulously crafted to streamline the process of data collection, model 
development, and execution of real-time trades.

Key components of the pipeline include:

- **Data Collector**: This component collects real-time trading information from Coinbase and stores it in a MongoDB database. An AWS Lambda process is employed to optimize data storage and retrieval.  
<br/>
- **Model Creator**: This system harnesses the stored data to train and backtest predictive models, ensuring performance validation. It uses MLflow to manage the machine learning pipeline, facilitating efficient model development and deployment.  
<br/>
- **Trader**: A component that carries out real-time trades on Coinbase, guided by the predictions made by the machine learning models. It also supports A/B testing in a production environment, allowing for continuous improvement based on data-driven insights.  
<br/>
- **Web Interface**: A user-friendly platform built with Next.js, React, JavaScript, CSS, and HTML that allows real-time monitoring of the performance of each model, contributing to better, more informed decision-making.  

Throughout the development of this project, there was a strong emphasis on 
cost-efficiency, reliability, and automatic recovery from failures to 
minimize the need for manual maintenance. This project provided an 
excellent opportunity to apply and further enhance skills in complex 
system design, data processing, and advanced machine-learning techniques.

