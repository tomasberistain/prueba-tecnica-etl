import sys


class Primeros100Nat:
    def __init__(self):
        self.numeros = set(range(1, 101))  # 1 al 100, inclusivo
        self.suma_original = 5050  # 100*101//2 SUMA DE LOS PRIMEROS 100 NÚMEROS
        self.suma_actual = self.suma_original
        self.extraido = None

    #usamos extract para extraer un número del conjunto de números.
    def extract(self, n):
        if not isinstance(n, int):
            raise ValueError("El valor debe ser un número entero.")

        if n < 1 or n > 100:
            raise ValueError("El número debe estar en el rango de 1 a 100, 100 incluído.")

        if n not in self.numeros:
            raise ValueError(f"El número {n} no está en el conjunto ")

        self.numeros.remove(n)
        self.suma_actual -= n
        self.extraido = n
        return True

    def numero_faltante(self):
        if self.extraido is None:
            raise ValueError("Todavía no se ha extraído ningún número")
        return self.suma_original - self.suma_actual

    def __str__(self):
        if self.extraido is not None:
            return f"Se extrajo el {self.extraido}. Ahora al conjunto le falta el número: {self.numero_faltante()}"
        return "Aún no se ha extraído ningún número"


def main():
    # SI HAY MAS DE TRES ARGUMENTOS, MAS QUE EL NOMBRE DEL PROGRAMA Y EL NUMERO, MUESTRA UN MENSAJE DE INSTRUCTIVO
    if len(sys.argv) != 2:
        print("Debes colocar únicamente:")
        print("   python main.py <número>")
        print("Por ejemplo:")
        print("   python main.py 18")
        sys.exit(1)

    try:
        numero = int(sys.argv[1])
    except ValueError:
        print("Debe escribirse un número entero")
        sys.exit(1)

    try:
        conjunto = Primeros100Nat()
        conjunto.extract(numero)
        print(conjunto)
    except ValueError as e:
        print("Error:", e)
        sys.exit(1)
    except Exception as e:
        print("Error inesperado:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()