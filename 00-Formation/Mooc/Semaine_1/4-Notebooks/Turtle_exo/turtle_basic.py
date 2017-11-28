
# coding: utf-8

# <style>div.title-slide {    width: 100%;    display: flex;    flex-direction: row;            /* default value; can be omitted */    flex-wrap: nowrap;              /* default value; can be omitted */    justify-content: space-between;}</style><div class="title-slide">
# <span style="float:left;">Licence CC BY-NC-ND</span>
# <span>Thierry Parmentelat &amp; Arnaud Legout</span>
# <span><img src="media/both-logos-small-alpha.png" style="display:inline" /></span>
# </div>

# # Dessiner un carré

# ## Exercice - niveau intermédiaire

# Voici un tout petit programme qui dessine un carré.

# Il utilise le module `turtle`, conçu précisément à des fins pédagogiques. Pour des raisons techniques, le module `turtle` n'est **pas disponible** au travers de la plateforme FUN.

# **Il est donc inutile d'essayer d'exécuter ce programme depuis le notebook**. L'objectif de cet exercice est plutôt de vous entraîner à télécharger ce programme en utilisant le menu *"File -> Download as -> python"*, puis à le charger dans votre IDLE pour l'exécuter sur votre machine.

# **Attention** également à sauver le programme téléchargé **sous un autre nom** que `turtle.py`,  car sinon vous allez empêcher python de trouver le module standard `turtle`&nbsp;; appelez-le par exemple `turtle_basic.py`.

# In[ ]:


# on a besoin du module turtle
import turtle

# On commence par définir une fonction qui dessine un carré de coté `length`

# In[ ]:

def square(length):
    "have the turtle draw a square of side <length>"
    for side in range(4):
        turtle.color('blue', 'green')
        turtle.forward(length)
        turtle.left(90)


# Maintenant on commence par initialiser la tortue

# In[ ]:


turtle.reset()


# On peut alors dessiner notre carré

# In[ ]:


square(100)


# Et pour finir on attend que l'utilisateur clique dans la fenêtre de la tortue, et alors on termine

# In[ ]:


turtle.exitonclick()


# ## Exercice - niveau avancé

# Naturellement vous pouvez vous amuser à modifier ce code pour dessiner des choses un peu plus amusantes. 
# 
# Dans ce cas, commencez par chercher "*module python turtle*" dans votre moteur de recherche favori, pour localiser la documentation du module [`turtle`](https://docs.python.org/3/library/turtle.html).
# 
# Vous trouverez quelques exemples pour commencer ici&nbsp;:
#  * [turtle_multi_squares.py](media/turtle_multi_squares.py) pour dessiner des carrés à l'emplacement de la souris en utilisant plusieurs tortues&nbsp;;
#  * [turtle_fractal.py](media/turtle_fractal.py) pour dessiner une fractale simple&nbsp;;
#  * [turtle_fractal_reglable.py](media/turtle_fractal_reglable.py) une variation sur la fractale, plus paramétrable.
#  
