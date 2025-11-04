In this project, I used a simple recommendation strategy based on co-occurrence. 
It means that if two products are often bought or seen together, the system suggests one when the other appears. If no similar cases are found, it recommends products from the same category. It’s a basic but clear way to show how recommendations can work with a graph.

To make this project ready for real use, I would improve a few things : 
First, I would make the ETL run automatically and not directly inside the FastAPI route. Then, I would add real algorithms from Neo4j GDS like Personalized PageRank to make recommendations more relevant. I would also improve error handling, add logs, and make the API faster and safer. 
Finally, I would connect it to a small web app so users can see their own recommendations easily.

**You can see the screenshots below :**


**1.**
<img width="620" height="64" alt="Capture d’écran 2025-11-03 à 19 10 46" src="https://github.com/user-attachments/assets/ae038ce3-0557-4434-99dd-74176e3dd2e6" />

**2.**
<img width="1353" height="385" alt="Capture d’écran 2025-11-03 à 19 10 30" src="https://github.com/user-attachments/assets/35c2a35e-fa90-48cf-bb11-8663c5ffa64d" />

**3.**
<img width="1353" height="386" alt="Capture d’écran 2025-11-03 à 19 10 23" src="https://github.com/user-attachments/assets/f1c9c442-14bb-45ba-81a3-af4cd2aa11a1" />

**4.**
<img width="1217" height="348" alt="Capture d’écran 2025-11-03 à 19 10 17" src="https://github.com/user-attachments/assets/5886b054-8d40-45f3-b090-8a401fb53e3e" />
