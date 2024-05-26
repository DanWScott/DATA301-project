import pickle
import dask.dataframe as df
import dask.bag as db
from dask.dataframe import from_pandas

# IMPORT THE PICKLE FILE.
with open('ingr_map.pkl', 'rb') as file:
    ingr_indices = pickle.load(file)
ingr_indices = from_pandas(ingr_indices, npartitions=1)

# IMPORT DATASETS
recipes = df.read_csv('PP_recipes.csv')
reviews = df.read_csv('RAW_interactions.csv')

# FIND THE IDS OF MY INGREDIENTS
veggie_lasagna = {"sweet potato",
            "pumpkin",
            "olive oil",
            "paprika",
            "salt",
            "pepper",
            "onion",
            "garlic",
            "tomato",
            "cottage cheese",
            "oregano",
            "basil pesto",
            "lasagna sheet",
            "mozzarella"}

chicken_fajitas = {"chicken breast",
            "red onion",
            "red pepper",
            "chili pepper",
            "paprika",
            "ground coriander",
            "ground cumin",
            "garlic",
            "olive oil",
            "lime juice",
            "tabasco sauce",
            "tortilla",
            "salsa"}

noodles = {"red onion",
           "noodle",
           "soy sauce",
           "oyster sauce",
           "mirin",
           "hoisin sauce",
           "sesame oil",
           "red pepper",
           "onion",
           "green bean",
           "broccoli",
           "bok choy"}

curry = {"coconut oil",
         "onion",
         "garlic",
         "ginger",
         "curry leaf",
         "curry powder",
         "cinnamon stick",
         "salt",
         "chili powder",
         "paprika",
         "chicken",
         "serrano pepper",
         "tomato",
         "brown sugar",
         "lime juice",
         "coconut milk",
         "water",
         "rice"}

my_ingrs = veggie_lasagna
            

ingredient_subset = ingr_indices[ ingr_indices['processed'].isin(my_ingrs)]
my_ingredient_ids = set(ingredient_subset['id'].compute().tolist())

# GET ALL THE REVIEWS SUFFICIENTLY HIGHLY RATED
RATING_THRESHOLD = 4.0
reviews = reviews.groupby(['recipe_id'])['rating'].mean()
good_reviews = reviews[reviews >= RATING_THRESHOLD]
good_recipe_ids = set(good_reviews.index.compute().tolist())
good_recipes = recipes[ recipes['id'].isin(good_recipe_ids) ]

# FIGURE OUT EACH RECIPE'S JACCARD DISTANCE FROM MY INGREDIENTS LIST
SIMILARITY_THRESHOLD = 0.2
def jaccard_similarity(ingredient_list):
    ingredient_set = set(ingredient_list)
    intersect = ingredient_set.intersection(my_ingredient_ids)
    union = ingredient_set.union(my_ingredient_ids)
    return len(intersect)/len(union)

good_recipes['similarity'] = good_recipes['ingredient_ids'].apply(lambda x: jaccard_similarity(eval(x)), meta=('ingredient_ids', float))
similar_recipes = good_recipes[ good_recipes['similarity'] >= SIMILARITY_THRESHOLD ]


# JUST FOR FUN - WHAT ARE THE MOST SIMILAR RECIPES?

similars = similar_recipes.groupby(['id'])['similarity'].mean().nlargest(10)
similar_recipe_ids = similars.compute().index.tolist()
raw_recipes = df.read_csv('RAW_recipes.csv')
top_similar_recipes = raw_recipes[ raw_recipes['id'].isin(similar_recipe_ids) ]
names = top_similar_recipes['name'].compute().tolist()
for name in names:
    print(name)

