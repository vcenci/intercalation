import csv
import struct
import time
import os

product_id_idx = None
category_id_idx = None
brand_idx = None
user_id_idx = None
event_type_idx = None
event_time_idx = None
price_idx = None
batch_size = 5000000
TEMP_DIR = r'D:\Arquivo'
PRODUCT_STRUCT = 'QQ20sQd20sc'
USER_STRUCT = 'QQ20s23sc'
INDEX_STRUCT = 'QQ'
BLOCK_SIZE = 5000000

 
def menu():
    while True:
        option = input("Digite a opção desejada: \n1 - Particionar, classificar e intercalar\n2 - Listar registro\n3 - Inserir registro\n4 - Remover registro\n5 - Listar todos os dados\n6 - Produto mais procurado\n7 - Usuário mais ativo\n0 - Sair\n")
        if option == '1':
            intercalate_files(create_temp_files())
        elif option == '2':
            list_entry()
        elif option == '3':
            add_entry()
        elif option == '4':
            remove_entry()
        elif option == '5':
            list_entries()
        elif option == '6':
            most_searched_product()
        elif option == '7':
            most_active_by_user()
        elif option == '0':
            break

def most_searched_product():
    id, searched = most_searched('products_idx.bin')
    print(f"O produto com mais consultas é o id {id}, com {searched} consultas")

def most_active_by_user():
    id, searched = most_searched('users_idx.bin')
    print(f"O usuário com mais movimentações é o id {id}, com {searched} registros")

def most_searched(filepath):
    with open(os.path.join(TEMP_DIR, filepath), 'rb') as file:
        searched = None
        id = None
        previous_key = None
        
        while True:
            record = file.read(struct.calcsize(INDEX_STRUCT))
            if len(record) < struct.calcsize(INDEX_STRUCT):
                break
            
            data = struct.unpack(INDEX_STRUCT, record)
            current_key = data[0]
            current_id = data[1]
            
            if previous_key is not None:
                searched_aux = current_key - previous_key
                if searched is None or searched_aux > searched:
                    searched = searched_aux
                    id = current_id

            previous_key = current_key
        file.close()
    return id, searched

def list_entry():
    type = int(input("Qual tipo de registro deseja listar? 1 - Produtos, 2 Usuarios: "))
    if type == 1 or type == 2:
        id = int(input("Digite o id do registro: "))
        if type == 1:
            filepath = os.path.join(TEMP_DIR, f'products.bin')
            index_filepath = os.path.join(TEMP_DIR, f'products_idx.bin')
            struct_format = PRODUCT_STRUCT
        else:
            filepath = os.path.join(TEMP_DIR, f'users.bin')
            index_filepath = os.path.join(TEMP_DIR, f'users_idx.bin')
            struct_format = USER_STRUCT
    else:
        print("Opção inválida")
        return

    found, key = binary_search(int(id), os.path.join(TEMP_DIR, index_filepath), INDEX_STRUCT)
    if not found:
        nome = 'Produto' if type == 1 else 'Usuário'
        print(f"{nome} não encontrado!")
        return

    first_run = True
    entry_size = struct.calcsize(struct_format)
    offset = key * entry_size
    with open(os.path.join(TEMP_DIR, filepath), 'rb') as file:
        file_size = file.seek(0, os.SEEK_END)
        while True:
            if offset < 0 or offset >= file_size:
                break
            file.seek(offset)
            record = file.read(entry_size)
            if len(record) < entry_size:
                break
            data = list(struct.unpack(struct_format, record))
            if first_run and data[-1] == b"S":
                nome = 'Produto' if type == 1 else 'Usuário'
                print(f"{nome} não encontrado!!!")
                break
            if data[1] != int(id):
                break
            print(data)
            first_run = False
            offset -= entry_size
    file.close()

def list_entries():
    limit = None
    type = int(input("Qual arquivo de dados deseja listar? 1 - Produtos, 2 - Índices dos produtos, 3 - Usuários, 4 Índices dos usuários: "))
    if type == 1 or type == 2 or type == 3 or type == 4:
        if type == 1:
            filepath = os.path.join(TEMP_DIR, f'products.bin')
            struct_format = PRODUCT_STRUCT
        elif type == 2:
            filepath = os.path.join(TEMP_DIR, f'products_idx.bin')
            struct_format = INDEX_STRUCT
        elif type == 3:
            filepath = os.path.join(TEMP_DIR, f'users.bin')
            struct_format = USER_STRUCT
        elif type == 4:
            filepath = os.path.join(TEMP_DIR, f'users_idx.bin')
            struct_format = INDEX_STRUCT
        
        if input("Limitar a quantidade de registros? (S/N): ").lower() == "s":
            limit = int(input("Digite a quantidade de registros: "))
    else:
        print("Opção inválida")
        return

    count = 0
    with open(os.path.join(TEMP_DIR, filepath), 'rb') as file:
        while True:
            record = file.read(struct.calcsize(struct_format))
            if len(record) < struct.calcsize(struct_format):
                break
            data = struct.unpack(struct_format, record)
            if data[-1] == b"S":
                continue
            print(data)
            if limit is not None and count == limit:
                break
            count += 1
    file.close()

def remove_entry():
    type = int(input("Qual arquivo de dados deseja remover o registro? 1 - Produtos, 2 - Usuários: "))
    if type == 1:
        filepath = os.path.join(TEMP_DIR, f'products.bin')
        index_filepath = os.path.join(TEMP_DIR, f'products_idx.bin')
        struct_format = PRODUCT_STRUCT
    elif type == 2:
        filepath = os.path.join(TEMP_DIR, f'users.bin')
        index_filepath = os.path.join(TEMP_DIR, f'users_idx.bin')
        struct_format = USER_STRUCT
    else:
        print("Opção inválida")
        return

    id = int(input("Digite o id do registro: "))
    found, key = binary_search(id, index_filepath, INDEX_STRUCT)

    if not found:
        print("Registro não encontrado")
        return 

    print("Marcando os registros como removidos no arquivo de dados")
    with open(filepath, 'r+b') as file:
        file_size = file.seek(0, os.SEEK_END)
        entry_size = struct.calcsize(struct_format)
        while True:
            offset = key * entry_size
            if offset < 0 or offset >= file_size:
                break
            file.seek(offset)
            record = file.read(entry_size)
            if len(record) < entry_size:
                break
            record = list(struct.unpack(struct_format, record))
            if record[1] != id:
                break
            record[-1] = "S".encode('utf-8')[:1]
            file.seek(offset)
            file.write(struct.pack(struct_format, *record))
            key -= 1
        file.close()

def binary_search(id, filepath, struct_format):
    entry_size = struct.calcsize(struct_format)
    with open(filepath, 'rb') as file:
        file_size = file.seek(0, 2)  
        num_records = file_size // entry_size
        
        left, right = 0, num_records - 1
        
        while left <= right:
            mid = (left + right) // 2
            
            file.seek(mid * entry_size)
            record = file.read(entry_size)
            data = list(struct.unpack(struct_format, record))
            
            if data[1] == id:
                file.close()       
                return True, data[0]
            
            elif data[1] < id:
                left = mid + 1
            else:
                right = mid - 1
        file.close()
        return False, left

def add_entry():
    type = int(input("Qual tipo de registro deseja adicionar? 1 - Produtos, 2 - Usuários: "))
    if type == 1:
        struct = [
            0,
            int(input("Digite o id do produto: ")),
            input("Digite a marca do produto: ").ljust(20, " ").encode('utf-8')[:20],
            int(input("Digite o id da categoria do produto: ")),
            float(input("Digite o preço do produto: ")),
            input("Digite o tipo do evento: ").ljust(20, " ").encode('utf-8')[:20], 
            "N".encode('utf-8')[:1]
        ]
        indexfilepath = os.path.join(TEMP_DIR, f'products_idx.bin')
        datafilepath = os.path.join(TEMP_DIR, f'products.bin')
        struct_format = PRODUCT_STRUCT
    elif type == 2:
        struct = [
            0,
            int(input("Digite o id do usuário: ")),
            input("Digite o tipo de evento: ").encode('utf-8')[:20],
            input("Digite o horário do evento: ").encode('utf-8')[:23],
            "N".encode('utf-8')[:1]
        ]
        indexfilepath = os.path.join(TEMP_DIR, f'users_idx.bin')
        datafilepath = os.path.join(TEMP_DIR, f'users.bin')
        struct_format = USER_STRUCT
    else:
        print("Opção inválida")
        return

    found, key = binary_search(struct[1], indexfilepath, INDEX_STRUCT)

    struct[0] = key
    if found:
        struct[0] = key + 1
    
    start_time = time.time() 
    print("Inserindo no arquivo de dados")
    insert_to_file(tuple(struct), struct_format, datafilepath, found)
    print(f"Dado inserido em {time.time() - start_time} segundos")

    start_time = time.time() 
    print("Reescrevendo os índices")
    rewrite_indexes(datafilepath, struct_format, 'products' if type == 1 else 'users')
    print(f"Índices reescritos em {time.time() - start_time} segundos")

def rewrite_indexes(data_filepath, format_data, type):
    if type == 'users':
        index_filepath = os.path.join(TEMP_DIR, f'users_idx.bin')
    elif type == 'products':
        index_filepath = os.path.join(TEMP_DIR, f'products_idx.bin')
    if os.path.exists(index_filepath):
        os.remove(index_filepath)

    count = 0
    with open(data_filepath, 'r+b') as file:
        entry_size = struct.calcsize(format_data)
        previous_id = None
        while True:
            record = file.read(entry_size)
            if len(record) < entry_size:
                break
            data = list(struct.unpack(format_data, record))
            if data[1] != previous_id and previous_id is not None:
                create_partial_index(previous_id, count - 1, type)
            previous_id = data[1]
            count += 1


def insert_to_file(struct_data, struct_format, filepath, found):
    with open(filepath, 'r+b') as file:
        entry_size = struct.calcsize(struct_format)  
        offset = struct_data[0] * entry_size
        print(f'Produto será inserido na chave {struct_data[0]} offset {offset}')

        file.seek(0, os.SEEK_END)
        file_size = file.tell()

        bytes_to_shift = file_size - offset

        current_position = offset
        while bytes_to_shift > 0:
            read_size = min(BLOCK_SIZE, bytes_to_shift)
            file.seek(current_position) 
            data_to_shift = file.read(read_size) 

            file.seek(current_position + entry_size) 

            file.write(data_to_shift)

            current_position += read_size
            bytes_to_shift -= read_size
        
        for i in range(struct_data[0] + 1, (file_size // entry_size) + 1):
            file.seek(i * entry_size)
            record = file.read(entry_size)

            if len(record) < entry_size:
                break 

            unpacked = list(struct.unpack(struct_format, record))
            unpacked[0] += 1 
                
            file.seek(i * entry_size)
            file.write(struct.pack(struct_format, *unpacked))
        
        file.seek(offset) 

        file.write(struct.pack(struct_format, *struct_data))

    file.close()

def intercalate_files(num_files):
    register_products_key = 0
    register_users_key = 0
    last_user_id = 0
    last_product_id = 0
    tmp_readers_products = {}
    tmp_readers_users = {}
    register_products_queue = {}
    register_users_queue = {}

    for i in range(num_files):
        tmp_readers_products[i] = open(os.path.join(TEMP_DIR, f'temp_products_{i}.bin'), 'rb')
        tmp_readers_users[i] = open(os.path.join(TEMP_DIR, f'temp_users_{i}.bin'), 'rb')

        product_record = tmp_readers_products[i].read(struct.calcsize(PRODUCT_STRUCT))
        users_record = tmp_readers_users[i].read(struct.calcsize(USER_STRUCT)) 
        if product_record or users_record: 
            register_products_queue[i] = struct.unpack(PRODUCT_STRUCT, product_record) 
            register_users_queue[i] = struct.unpack(USER_STRUCT, users_record) 

    products_filepath = os.path.join(TEMP_DIR, f'products.bin')
    users_filepath = os.path.join(TEMP_DIR, f'users.bin')

    with open(products_filepath, 'wb') as products_file, open(users_filepath, 'wb') as users_file:
        start_time = time.time() 
        print('Início da intercalação dos arquivos.')
        while register_products_queue or register_users_queue:            
            min_product_key = min(register_products_queue, key=lambda x: int(register_products_queue[x][1]))
            min_product = register_products_queue[min_product_key] 

            min_user_key = min(register_users_queue, key=lambda x: int(register_users_queue[x][1])) 
            min_user = register_users_queue[min_user_key]

            temp_product_data = (
                register_products_key,
                min_product[1],
                min_product[2],
                min_product[3],
                min_product[4],
                min_product[5],
                min_product[6]
            )
            products_file.write(struct.pack(PRODUCT_STRUCT, *temp_product_data))

            temp_users_data = (
                register_users_key,
                min_user[1],
                min_user[2],
                min_user[3],
                min_user[4]
            )
            users_file.write(struct.pack(USER_STRUCT, *temp_users_data))

            next_product_record = tmp_readers_products[min_product_key].read(struct.calcsize(PRODUCT_STRUCT))
            next_user_record = tmp_readers_users[min_user_key].read(struct.calcsize(USER_STRUCT))
            if next_product_record:
                next_record = struct.unpack(PRODUCT_STRUCT, next_product_record)
                register_products_queue[min_product_key] = next_record
            else:
                del register_products_queue[min_product_key] 
            if next_user_record:
                next_record = struct.unpack(USER_STRUCT, next_user_record)
                register_users_queue[min_user_key] = next_record
            else:
                del register_users_queue[min_user_key]

            if register_users_key != 0 and last_user_id != min_user[1]:
                create_partial_index(last_user_id, register_users_key - 1, 'users')
            if register_users_key != 0 and last_product_id != min_product[1]:
                create_partial_index(last_product_id, register_products_key - 1, 'products')

            last_user_id = min_user[1]
            last_product_id = min_product[1]

            register_users_key += 1    
            register_products_key += 1    

    for reader in tmp_readers_products.values():
        reader.close()
    for reader in tmp_readers_users.values():
        reader.close()

    elapsed_time = time.time() - start_time
    print(f'Arquivos intercalados com sucesso em {elapsed_time:.2f}.')

def create_partial_index(id, register_key, type):
    if type == 'users':
        filepath = os.path.join(TEMP_DIR, f'users_idx.bin')
    elif type == 'products':
        filepath = os.path.join(TEMP_DIR, f'products_idx.bin')
    with open(filepath, 'ab') as idx_file:
        temp_data = (
            register_key,
            id
        )
        idx_file.write(struct.pack(INDEX_STRUCT, *temp_data))

def create_temp_files():
    global product_id_idx, category_id_idx, brand_idx, user_id_idx, event_type_idx, event_time_idx, price_idx
    try:
        with open('2019-Oct.csv', 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)

            product_id_idx = header.index('product_id')
            category_id_idx = header.index('category_id')
            brand_idx = header.index('brand')
            price_idx = header.index('price')
            user_id_idx = header.index('user_id')
            event_type_idx = header.index('event_type')
            event_time_idx = header.index('event_time')

            print('Início do processamento do arquivo CSV.')

            batch_number = 0
            current_batch = []

            start_time = time.time() 
            for cols in reader:
                current_batch.append(cols)

                if len(current_batch) == batch_size:
                    write_batch_to_files(current_batch, batch_number, start_time)
                    batch_number += 1
                    current_batch = []
                    start_time = time.time()

            if current_batch:
                write_batch_to_files(current_batch, batch_number, start_time)
        print('Processamento completo.')
        return batch_number
    except Exception as e:
        print(f'Ocorreu um erro: {e}')
        return 0


def write_batch_to_files(batch, batch_number, start_time):
    temp_file_products_path = os.path.join(TEMP_DIR, f'temp_products_{batch_number}.bin')
    temp_file_users_path = os.path.join(TEMP_DIR, f'temp_users_{batch_number}.bin')

    print(f'Processando arquivo {batch_number} com {len(batch)} linhas.')
    with open(temp_file_products_path, 'wb') as temp_products_file, open(temp_file_users_path, 'wb') as temp_users_file:
        print(f'Salvando arquivo de usuários')
        batch.sort(key=lambda x: int(x[user_id_idx]))
        for cols in batch:
            temp_data = (
                0,
                int(cols[user_id_idx]), 
                (cols[event_type_idx].ljust(20, " ").encode('utf-8'))[:20], 
                (cols[event_time_idx].ljust(23, " ").encode('utf-8'))[:23],
                b"N"
            )
            temp_users_file.write(struct.pack(USER_STRUCT, *temp_data))

        print(f'Salvando arquivo de produtos')
        batch.sort(key=lambda x: int(x[product_id_idx]))
        for cols in batch:
            temp_data = (
                0,
                int(cols[product_id_idx]),
                (cols[brand_idx].ljust(20, " ").encode('utf-8'))[:20], 
                int(cols[category_id_idx]),
                float(cols[price_idx]),
                (cols[event_type_idx].ljust(20, " ").encode('utf-8'))[:20], 
                b"N"
            )
            temp_products_file.write(struct.pack(PRODUCT_STRUCT, *temp_data))
            
        elapsed_time = time.time() - start_time 
        print(f'Arquivo {batch_number} processado e gravado com {len(batch)} de linhas, em {elapsed_time:.2f} segundos.')

menu()