import threading
import time
from collections import defaultdict
from pathlib import Path
import os

def search_in_file(file_path, keywords, results, lock):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            content = file.read()
            for keyword in keywords:
                if keyword in content:
                    with lock:
                        results[keyword].append(file_path)
    except FileNotFoundError:
        print(f"Помилка: Файл {file_path} не знайдено.")
    except PermissionError:
        print(f"Помилка: Немає доступу до файлу {file_path}.")
    except Exception as e:
        print(f"Помилка під час обробки файлу {file_path}: {e}")


def thread_task(files, keywords, results, lock):
    for file in files:
        search_in_file(file, keywords, results, lock)


def get_optimal_thread_count():
    return max(1, os.cpu_count() or 8)


def main_threading(file_paths, keywords):
    start_time = time.time()
    num_threads = min(get_optimal_thread_count(), len(file_paths))
    threads = []
    results = defaultdict(list)
    lock = threading.Lock()
    
    chunk_size = len(file_paths) // num_threads
    file_chunks = [file_paths[i * chunk_size: (i + 1) * chunk_size] for i in range(num_threads)]
    file_chunks[-1].extend(file_paths[num_threads * chunk_size:]) 
    for i in range(num_threads):
        thread = threading.Thread(target=thread_task, args=(file_chunks[i], keywords, results, lock))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    end_time = time.time()
    print(f"Час виконання: {end_time - start_time:.2f} секунд")
    
    return results

if __name__ == "__main__":
    folder_path = "./test"
    keywords = ["воля", "Каламутними болотами", "Афоризми"] 
    file_paths = [str(f) for f in Path(folder_path).rglob("*.txt")]
    
    results = main_threading(file_paths, keywords)
    
    for keyword, files in results.items():
        print(f"Ключове слово '{keyword}' знайдено в файлах:")
        for file in files:
            print(f"  - {file}")
