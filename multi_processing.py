import multiprocessing
from collections import defaultdict
from pathlib import Path
import time

def search_in_file(file_path, keywords, results_queue):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            content = file.read()
            for keyword in keywords:
                if keyword in content:
                    results_queue.put((keyword, str(file_path)))
    except FileNotFoundError:
        print(f"Помилка: Файл {file_path} не знайдено.")
    except PermissionError:
        print(f"Помилка: Немає доступу до файлу {file_path}.")
    except Exception as e:
        print(f"Помилка під час обробки файлу {file_path}: {e}")

def process_task(files, keywords, results_queue):
    for file in files:
        search_in_file(file, keywords, results_queue)

def main_multiprocessing(file_paths, keywords):
    if not file_paths:
        print("Немає файлів для обробки.")
        return {}

    start_time = time.time() 
    num_processes = min(multiprocessing.cpu_count(), len(file_paths)) 
    chunk_size = (len(file_paths) + num_processes - 1) // num_processes  
    
    processes = []
    manager = multiprocessing.Manager()
    results_queue = manager.Queue()

    for i in range(num_processes):
        chunk = file_paths[i * chunk_size:(i + 1) * chunk_size]
        process = multiprocessing.Process(target=process_task, args=(chunk, keywords, results_queue))
        processes.append(process)
        process.start()
    
    for process in processes:
        process.join()
    
    results = defaultdict(list)
    while not results_queue.empty():
        keyword, file_path = results_queue.get()
        results[keyword].append(file_path)
    
    end_time = time.time()
    print(f"Час виконання: {end_time - start_time:.2f} секунд")
    
    return results

if __name__ == "__main__":
    folder_path = "./test"
    keywords = ["воля", "Каламутними болотами", "Афоризми"] 
    file_paths = list(Path(folder_path).rglob("*.txt"))
    
    results = main_multiprocessing(file_paths, keywords)
    
    for keyword, files in results.items():
        print(f"Ключове слово '{keyword}' знайдено в файлах:")
        for file in files:
            print(f"  - {file}")
