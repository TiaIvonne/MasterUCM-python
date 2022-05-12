from mrjob.job import MRJob, MRStep


def rango_edad(edad):
    rangos = {
        'DE 0 A 5 AÑOS': (0, 5),
        'DE 6 A 9 AÑOS': (6, 9),
        'DE 10 A 14 AÑOS': (10, 14),
        'DE 15 A 17 AÑOS': (15, 17),
        'DE 18 A 20 AÑOS': (18, 20),
        'DE 21 A 24 AÑOS': (21, 24),
        'DE 25 A 29 AÑOS': (25, 29),
        'DE 30 A 34 AÑOS': (30, 34),
        'DE 35 A 39 AÑOS': (35, 39),
        'DE 40 A 44 AÑOS': (40, 44),
        'DE 45 A 49 AÑOS': (45, 49),
        'DE 50 A 54 AÑOS': (50, 54),
        'DE 55 A 59 AÑOS': (55, 59),
        'DE 60 A 64 AÑOS': (60, 64),
        'DE 65 A 69 AÑOS': (65, 69),
        'DE 70 A 74 AÑOS': (70, 74),
        'MAYOR DE 74 AÑOS': (75, 100)
    }
    return rangos.get(edad, (-1, -1))


def entero(cadena):
    try:
        return int(cadena)
    except:
        return 0


def arma_lista(lista):
    accidentes, muertes = [], []
    for i in lista:
        accidentes.append(i[0])
        muertes.append(i[1])
    return [sum(accidentes), sum(muertes)]


class Cuenta(MRJob):
    def mapper(self, _, linea):
        accidentes, muertes = 0, 0
        row = linea.split(';')
        edad = rango_edad(row[10])
        lesividad = entero(row[12])
        if edad != (-1, -1):
            accidentes = 1
            if lesividad == 4:
                muertes = 1
            yield edad, [accidentes, muertes]
   
    def reducer(self, key, values):
        yield key, arma_lista(values)


if __name__ == '__main__':
    Cuenta.run()
