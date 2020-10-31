# Python

## Ambiente Virtual

> Instalar o pacote **virtualenv**.

```
$ pip install virtualenv
```

> Criar o ambiente virtual.

```
$ virtualenv nome_do_ambiente_virtual 
```

> Ativar o ambiente virtual.

- Windows

```
$ nome_do_ambiente_virtual\Scripts\activate
```

- Linux

```
$ source nome_do_ambiente_virtual/bin/activate
```

> Desativar o ambiente virtual.

```
$ deactivate
```

> Listar pacotes instalados no projeto.
```
$ pip freeze
```

> Salvar a lista de dependências para automatizar a instalação.
```
$ pip freeze > requirements.txt
```

> Instalar dependências do projeto, após clonar o repositório Git, por exemplo.
```
$ pip install -r requirements.txt
```
