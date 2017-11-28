# we need the turtle module
# Exercice pour dessiner des carrés à l'emplacement de la souris en utilisant plusieurs tortues ;

import turtle

# avoid calling range for each square
sides = ['east', 'north', 'west', 'south']

def square(the_turtle, length):
    "have the turtle draw a square of side <length>"
    for side in sides:
        the_turtle.forward(length)
        the_turtle.left(90)

# initialize
window = turtle.Screen()
window.title("Multi_turtle Vince&Mélo")

# create first turtle
vince = turtle.Turtle()
vince.color("pink")
vince.reset()

# second turtle
melo = turtle.Turtle()
melo.color("green")
melo.reset()

# alternate : turtle, twist and square size
contexts = ((vince, 15, 100,),
            (melo, 60, 30),
           )
# initialize alternating contexts
cycle = len(contexts)
counter = -1

# the callback triggered when a user clicks in x,y
def clicked(x, y):
    global counter
    counter += 1
    # alternate between the various contexts
    (turtle, twist, size) = contexts[counter % cycle]
    turtle.penup()
    turtle.goto(x, y)
    turtle.pendown()
    turtle.left(twist)
    square(turtle, size)

# arm callback
turtle.onscreenclick(clicked)

# user can quit by typing 'q'
turtle.onkey(turtle.bye, 'q')
turtle.listen()

# read & dispatch events
turtle.mainloop()