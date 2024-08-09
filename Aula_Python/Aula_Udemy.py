#formatação de strings

a = 'São Paulo F.C'
b = '3X Campeão da libertadores e do mundo'
c = 1.1
string = 'b={nome2} a={nome1} a={nome1} c={nome3:.1f}'
formato = string.format(
    nome1=a, nome2=b, nome3=c
)

print(formato)



#nome = input('Qual o seu nome? ')
# print(f'O seu nome é {nome}')

numero_1 = input('Digite um número: ')
numero_2 = input('Digite outro número: ')

int_numero_1 = int(numero_1)
int_numero_2 = int(numero_2)

print(f'A soma dos números é: {int_numero_1 + int_numero_2}')