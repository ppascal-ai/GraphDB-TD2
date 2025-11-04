In this project, I used a simple recommendation strategy based on co-occurrence. 
It means that if two products are often bought or seen together, the system suggests one when the other appears. If no similar cases are found, it recommends products from the same category. It’s a basic but clear way to show how recommendations can work with a graph.

To make this project ready for real use, I would improve a few things : 
First, I would make the ETL run automatically and not directly inside the FastAPI route. Then, I would add real algorithms from Neo4j GDS like Personalized PageRank to make recommendations more relevant. I would also improve error handling, add logs, and make the API faster and safer. 
Finally, I would connect it to a small web app so users can see their own recommendations easily.