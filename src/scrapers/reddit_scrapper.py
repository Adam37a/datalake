import praw
import pandas as pd
from datetime import datetime

# === Config Reddit API ===
reddit = praw.Reddit(
    client_id='dFeZjjZNU1HlQXBOOtF3LQ',
    client_secret='JaGVSoGXLNplolbSd6rGtq6Q62fyYA',
    user_agent='JobScraper/0.1 by wass',
    username='True-Environment5594',
    password='Reddit2000!!'
)

# === Liste des subreddits cibles ===
target_subreddits = ['forhire', 'datascience', 'cscareerquestions']

# === Mot-clé à rechercher ===
search_query = 'Python job'
post_limit = 500  # Nombre max de posts par subreddit

# === Récupération des posts ===
all_posts = []

for sub in target_subreddits:
    subreddit = reddit.subreddit(sub)
    print(f"🔎 Recherche dans r/{sub}...")

    for post in subreddit.search(search_query, limit=post_limit):
        all_posts.append({
            'subreddit': sub,
            'title': post.title,
            'author': str(post.author),
            'score': post.score,
            'num_comments': post.num_comments,
            'url': post.url,
            'created_utc': datetime.fromtimestamp(post.created_utc),
            'permalink': f"https://reddit.com{post.permalink}"
        })

# === Export final en CSV ===
df = pd.DataFrame(all_posts)
output_file = 'reddit_multi_subreddits.csv'
df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"✅ Scraping terminé : {len(df)} posts enregistrés dans {output_file}")