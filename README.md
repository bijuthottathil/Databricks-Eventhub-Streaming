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

![image](https://github.com/user-attachments/assets/ed3ba69b-507e-4959-a2f3-2d916f1e5316)

