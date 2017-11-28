#!/usr/bin/env python
# -*- coding: utf-8 -*-

def fibonacci(n):
    "retourne le nombre de fibonacci pour l'entier n"
    # pour les petites valeurs de n il n'y a rien à calculer
    if n <= 1:
        return 1
    # sinon on initialise f1 pour n-1 et f2 pour n-2
    f2, f1 = 1, 1
    # et on itère n-1 fois pour additionner
    for i in range(2, n + 1):
        f2, f1 = f1, f1 + f2
        print(i, f2, f1)
    # le résultat est dans f1
    return f1

entier = int(input("Entrer un entier "))

print(f"fibonacci({entier}) = {fibonacci(entier)}")
