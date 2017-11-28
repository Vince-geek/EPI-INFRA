from turtle import *
color('blue', 'lightgreen')
begin_fill()
while True:
    forward(250)
    left(160)
    if abs(pos()) < 1:
        break
end_fill()
done()