from flask import Flask, request, jsonify
from transformers import pipeline, BertTokenizer, BertForSequenceClassification
from textblob import TextBlob
import os
import datetime
import speech_recognition as sr
import pyttsx3
from googletrans import Translator
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
import subprocess
import torch
import torch.nn as nn
import torch.optim as optim
import random

app = Flask(__name__)

# Инициализация моделей
chatbot = pipeline('conversational', model='facebook/blenderbot-3B')
code_generator = pipeline('text-generation', model='codegen')
translator = Translator()
engine = pyttsx3.init()

# Модель для анализа настроений
tokenizer = BertTokenizer.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment')
sentiment_model = BertForSequenceClassification.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment')

# Переменные для хранения данных
user_data = {}
reminders = {}
user_interactions = {}
user_progress = {}
user_data_for_training = []

# Нейронная сеть для обучения
class SimpleNN(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(SimpleNN, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        out = self.fc1(x)
        out = self.relu(out)
        out = self.fc2(out)
        return out

# Инициализация модели и параметров обучения
input_size = 100  # количество признаков
hidden_size = 128
output_size = 3  # положительный, нейтральный, отрицательный
model = SimpleNN(input_size, hidden_size, output_size)
criterion = nn.CrossEntropyLoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Функции машинного обучения и геймификации
def train_bot(user_id, user_input):
    if user_id not in user_interactions:
        user_interactions[user_id] = []
    user_interactions[user_id].append(user_input)
    if len(user_interactions[user_id]) > 10:
        user_interactions[user_id].pop(0)

def recommend_based_on_interests(user_id):
    if user_id not in user_data or not user_data[user_id]["interests"]:
        return "У вас пока нет интересов для рекомендаций."

    interests = user_data[user_id]["interests"]
    interests_joined = ' '.join(interests)
    
    corpus = [interests_joined] + ['игры', 'книги', 'фильмы', 'музыка', 'наука', 'технологии']
    vectorizer = CountVectorizer().fit_transform(corpus)
    vectors = vectorizer.toarray()
    cosine_matrix = cosine_similarity(vectors)
    
    recommendations = cosine_matrix[0][1:]  # Сравниваем интересы пользователя с возможными темами
    topics = ['Игры', 'Книги', 'Фильмы', 'Музыка', 'Наука', 'Технологии']
    
    top_index = np.argmax(recommendations)
    return f"Рекомендуем вам обратить внимание на: {topics[top_index]}"

def chat_with_bot(user_input, user_id, lang='ru'):
    if user_id not in user_data:
        user_data[user_id] = {"interests": []}

    if "интерес" in user_input.lower():
        interest = user_input.lower().replace("интерес:", "").strip()
        if interest not in user_data[user_id]["interests"]:
            user_data[user_id]["interests"].append(interest)
        return f"Запомнил ваш интерес: {interest}"

    train_bot(user_id, user_input)  # Обучение на взаимодействиях пользователя
    
    translated_input = translator.translate(user_input, src=lang, dest='en').text
    response = chatbot(translated_input)
    response_text = response[0]['generated_text']
    translated_response = translator.translate(response_text, src='en', dest=lang).text

    update_user_progress(user_id, 'message_sent')
    return translated_response

def generate_code(prompt):
    generated_code = code_generator(prompt, max_length=100)
    return generated_code[0]['generated_text']

def add_code_to_file(file_path, code_snippet):
    with open(file_path, 'a') as file:
        file.write('\n' + code_snippet)
    return "Код успешно добавлен."

def self_improve(prompt):
    new_code = generate_code(prompt)
    error = apply_rules(new_code)
    if error:
        return error

    add_code_to_file(__file__, new_code)
    return "ИИ успешно сам себя улучшил."

def update_user_progress(user_id, action):
    if user_id not in user_progress:
        user_progress[user_id] = {'level': 1, 'points': 0, 'achievements': []}
    
    if action == 'message_sent':
        user_progress[user_id]['points'] += 10
    elif action == 'achievement_unlocked':
        if 'First Message' not in user_progress[user_id]['achievements']:
            user_progress[user_id]['achievements'].append('First Message')
    
    # Обновление уровня
    points = user_progress[user_id]['points']
    if points >= 100:
        user_progress[user_id]['level'] = 2
    elif points >= 200:
        user_progress[user_id]['level'] = 3

    return user_progress[user_id]

def analyze_sentiment(text):
    inputs = tokenizer(text, return_tensors='pt')
    outputs = sentiment_model(**inputs)
    sentiment = np.argmax(outputs.logits.detach().numpy())
    sentiments = ["отлично", "хорошо", "нейтрально", "плохо", "ужасно"]
    return sentiments[sentiment]

def interactive_game(user_input):
    if "игра" in user_input.lower():
        return "Давайте сыграем в викторину! Какой ваш любимый жанр игр?"
    return "Давайте попробуем другую игру."

def analyze_user_data(user_id):
    if user_id not in user_interactions:
        return "Нет данных для анализа."

    data = user_interactions[user_id]
    if not data:
        return "Нет данных для анализа."

    # Применение PCA и кластеризации
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(data)
    
    pca = PCA(n_components=2)
    X_reduced = pca.fit_transform(X.toarray())
    
    kmeans = KMeans(n_clusters=3)
    kmeans.fit(X_reduced)
    
    return "Анализ данных завершен."

# Функции для активного обучения
def train_model_on_interactions():
    global user_data_for_training
    if len(user_data_for_training) < 10:
        return "Недостаточно данных для обучения"

    inputs = torch.tensor([item[0] for item in user_data_for_training], dtype=torch.float32)
    labels = torch.tensor([item[1] for item in user_data_for_training], dtype=torch.long)

    outputs = model(inputs)
    loss = criterion(outputs, labels)
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

    return f"Модель обучена с потерями: {loss.item()}"

def process_user_input_for_training(user_input):
    # Преобразуем ввод пользователя в числовые признаки
    features = [random.random() for _ in range(input_size)]  # Псевдоданные
    label = random.randint(0, 2)  # Псевдометки
    user_data_for_training.append((features, label))

    if len(user_data_for_training) >= 10:
        return train_model_on_interactions()
    return "Данных недостаточно для обучения. Продолжайте вводить запросы."

def ask_user_for_feedback(user_id):
    if user_id not in user_interactions:
        return "Нет данных для уточнения."
    
    last_interaction = user_interactions[user_id][-1]
    return f"Как бы вы оценили это взаимодействие: {last_interaction}? (Введите положительно, нейтрально или отрицательно)"

def improve_based_on_feedback(user_id, feedback):
    label_map = {"положительно": 0, "нейтрально": 1, "отрицательно": 2}
    if feedback not in label_map:
        return "Некорректный отзыв. Пожалуйста, выберите между положительно, нейтрально или отрицательно."
    
    last_interaction = user_interactions[user_id][-1]
    features = [random.random() for _ in range(input_size)]  # Генерация признаков для обучения
    label = label_map[feedback]
    
    user_data_for_training.append((features, label))
    return process_user_input_for_training(last_interaction)

if __name__ == "__main__":
    app.run(debug=True)