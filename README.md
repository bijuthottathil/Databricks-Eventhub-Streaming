# Real Time Streaming with Azure Databricks and Event Hubs

First step is to create Event Hub in Azure Portal

![image](https://github.com/user-attachments/assets/da37fb8c-d958-41fb-9492-17d22e42b332)

![image](https://github.com/user-attachments/assets/d05b80b2-869d-4b5f-95e2-08877207d087)

![image](https://github.com/user-attachments/assets/a52c534d-e85a-4405-ab72-79ab5b57b2dd)

Now we need to add event hub
![image](https://github.com/user-attachments/assets/e79ce472-5cbe-43c1-8d9d-3395002a5e8f)

![image](https://github.com/user-attachments/assets/70d01c21-f972-4844-91a5-be00552d1be4)

In Data Explorer, you will get option to simulate json request and post it

![image](https://github.com/user-attachments/assets/b99cf861-d8be-43b6-a106-f6d6a7586284)

You can send sample data and see how events are generating in view events option

![image](https://github.com/user-attachments/assets/cedc30c7-0133-4285-a793-e3347def117e)

Now move to Databricks and start cluster

Already I have created Catalogs and Schema(Bronze) to capture data in Medallion architechture

![image](https://github.com/user-attachments/assets/7cc1b812-fa6a-4f62-a9a6-654999d6fa03)

To support eventhub in databricks, we need to add below library in cluster
![image](https://github.com/user-attachments/assets/25d8001e-5759-45a1-8d64-55e390186e85)


To integrate this eventhub keys in Databricks, we neet to create a policy to listen the event 

![image](https://github.com/user-attachments/assets/47b97c18-c8fd-48fb-96dc-d12185886fc7)

We have to use primary key in databricks note book while connecting to event hub

![image](https://github.com/user-attachments/assets/21b87e17-2129-406d-833b-712101734b62)

Created a notebook to connect with Event Hub

You can see event hub connection string is mentioned below
![image](https://github.com/user-attachments/assets/6ab683f4-efab-40f2-a72d-474954803d7b)

I am using managed tables. You can see those tables mounted in DBFS

![image](https://github.com/user-attachments/assets/e141532c-71dc-4bb5-bee7-625b0907a3d4)


I will be using custom payload option to run with below weather specific json file

![image](https://github.com/user-attachments/assets/bd85ec20-ff41-4b98-8040-71aad7ddcdf2)


Before posting any events. Make sure that read stream and write steam are running in notebook

We did not post any data. 

![image](https://github.com/user-attachments/assets/e377320a-f43f-4c42-a95f-1a8a98c380fc)

Now I will be posting 1 json file
{
    "country": "USA",
    "city": "New York",
    "date": "2023-05-11",
    "temperature": 10,
    "humidity": 54,
    "windSpeed": 7,
    "windDirection": "NW",
    "precipitation": 0,
    "cloudCover": 20,
    "visibility": 10,
    "pressure": 1013,
    "dewPoint": 10,
    "uvIndex": 5,
    "sunrise": "05:30",
    "sunset": "20:15",
    "moonrise": "22:00",
    "moonset": "08:00",
    "moonPhase": "Waning Gibbous",
    "conditions": "Partly Cloudy",
    "icon": "partly-cloudy-day"
}

![image](https://github.com/user-attachments/assets/49b736a9-ff21-433e-a175-553b99aa23f9)


Automatically databricks read the data and created a delta table in DBFS

![image](https://github.com/user-attachments/assets/7fb15e7e-62ad-42b0-a19a-ec460a757c58)

Created a temp view to access and see delta table records

![image](https://github.com/user-attachments/assets/b5eeb49d-87b7-4b06-9bea-c774d9feef25)

posting few more json files after changing values
{
    "country": "USA",
    "city": "New York",
    "date": "2023-05-11",
    "temperature": 90,
    "humidity": 54,
    "windSpeed": 70,
    "windDirection": "NW",
    "precipitation": 0,
    "cloudCover": 20,
    "visibility": 10,
    "pressure": 1013,
    "dewPoint": 10,
    "uvIndex": 5,
    "sunrise": "05:30",
    "sunset": "20:15",
    "moonrise": "22:00",
    "moonset": "08:00",
    "moonPhase": "Waning Gibbous",
    "conditions": "Partly Cloudy",
    "icon": "partly-cloudy-day"
}

Automatically it added next record

![image](https://github.com/user-attachments/assets/1dff1a81-52c0-40d1-b51a-500b4cd86819)

3 rd record also added

![image](https://github.com/user-attachments/assets/33b46fc7-445d-4e1c-9082-6374182125f6)

Now we will focus on silver layer

Based on the input data , created a new schema in Silver layer

![image](https://github.com/user-attachments/assets/2af65d8a-1a1a-4e54-8047-654cce70f7db)

But we will be taking only required fields required for business

After submitting few more weather data, specific entries are stored to Silver layer Delta table

![image](https://github.com/user-attachments/assets/274cdc9a-c86f-4b15-a6fc-85c3f795860e)



![image](https://github.com/user-attachments/assets/bca8ad90-a1b9-44b7-8006-644184e3fc36)


In next step we will create gold layer and aggregate data


![image](https://github.com/user-attachments/assets/76e87119-2f57-4bf2-9dc5-c146d26d4310)


