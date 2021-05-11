# AWS Big Data Project

This project was given to me as an assignment as part of a course during my bechelor dgree.
In the course we used AWS and Hadoop to work with large amount of data. We learned the basics and the importance of ML.

In this assignment we generated a knowledge-base for Hebrew word-prediction system, based on Google 3-Gram Hebrew dataset, using Amazon Elastic Map-Reduce (EMR) and Hadoop. The produced knowledge-base indicates the probability of each word trigram found in the corpus.

In this assignment, we implemented a held out method, named deleted estimation:

<img width="578" alt="Screen Shot 2021-05-11 at 16 50 27" src="https://user-images.githubusercontent.com/44983890/117826867-2cf56a00-b279-11eb-8a85-2283d9ea1ef2.png">

Where:

- N is the number of n-grams in the whole corpus.
- Nr0 is the number of n-grams occuring r times in the first part of the corpus.
- Tr01 is the total number of those n-grams from the first part (those of Nr0) in the second part of the corpus.
- Nr1 is the number of n-grams occuring r times in the second part of the corpus.
- Tr10 is the total number of those n-grams from the second part (those of Nr1) in the first part of the corpus.
