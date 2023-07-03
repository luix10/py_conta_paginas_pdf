import os
import PyPDF2
import csv
import threading
import argparse


class ContadorPaginasThread(threading.Thread):
    def __init__(self, arquivo_pdf, resultado_compartilhado, resultado_compartilhado_erros, numero_tarefa):
        super().__init__()
        self.arquivo_pdf = arquivo_pdf
        self.resultado_compartilhado = resultado_compartilhado
        self.resultado_compartilhado_erros = resultado_compartilhado_erros
        self.numero_tarefa = numero_tarefa

    def run(self):
        try:
            with open(self.arquivo_pdf, 'rb') as arquivo:
                leitor_pdf = PyPDF2.PdfReader(arquivo)
                paginas = len(leitor_pdf.pages)
                self.resultado_compartilhado[self.arquivo_pdf] = paginas
                print(f"Tarefa {self.numero_tarefa} terminada. Arquivo: {self.arquivo_pdf}, Páginas: {paginas}")
        except Exception as e:
            self.resultado_compartilhado_erros[self.arquivo_pdf] = self.arquivo_pdf
            print(f"Erro na tarefa {self.numero_tarefa}. Arquivo: {self.arquivo_pdf}. Erro: {e}")

def gerar_relatorio_csv(diretorio, num_tarefas_simultaneas):
    resultado_compartilhado = {}
    resultado_compartilhado_erros = {}

    arquivos_pdf = []

    # Percorre todos os arquivos e subdiretórios do diretório informado
    for raiz, diretorios, arquivos in os.walk(diretorio):
        for arquivo in arquivos:
            # Verifica se o arquivo tem extensão pdf
            if arquivo.lower().endswith('.pdf'):
                caminho_completo_arquivo = os.path.join(raiz, arquivo)
                arquivos_pdf.append(caminho_completo_arquivo)

    threads = []
    total_tarefas = len(arquivos_pdf)
    numero_tarefas = 0

    # Percorre a lista de arquivos PDF e cria uma nova thread para contar as páginas de cada um
    for arquivo_pdf in arquivos_pdf:
        numero_tarefas += 1
        # Cria uma nova thread para contar as páginas do arquivo PDF
        thread = ContadorPaginasThread(arquivo_pdf, resultado_compartilhado, resultado_compartilhado_erros, numero_tarefa=numero_tarefas)
        thread.start()
        threads.append(thread)

        # Verifica se já existem pelo menos num_tarefas_simultaneas threads em execução
        while threading.active_count() > num_tarefas_simultaneas:
            pass

    # Aguarda o término de todas as threads
    for thread in threads:
        thread.join()

    # Escreve os resultados no arquivo CSV
    with open(os.path.join(diretorio, 'contagem_paginas.csv'), mode='w', newline='', encoding='utf-8') as relatorio_csv:
        writer = csv.writer(relatorio_csv)
        writer.writerow(['Arquivo', 'Páginas'])
        for arquivo, paginas in resultado_compartilhado.items():
            writer.writerow([arquivo, paginas])

    # Escreve os erros no arquivo CSV
    if len(resultado_compartilhado_erros)>0:
        with open(os.path.join(diretorio, 'contagem_erros.csv'), mode='w', newline='', encoding='utf-8') as erros_csv:
            writer = csv.writer(erros_csv)
            writer.writerow(['Arquivo'])
            for arquivo in resultado_compartilhado_erros.items():
                writer.writerow([arquivo])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Contagem de páginas de arquivos PDF')
    parser.add_argument('diretorio', type=str, help='diretório a ser pesquisado')
    parser.add_argument('--tarefas', type=int, default=3, help='número de tarefas simultâneas')
    args = parser.parse_args()

    gerar_relatorio_csv(args.diretorio, args.tarefas)
