import json
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime

# Set seaborn style
sns.set_theme(style="whitegrid")

# Read the latency data
with open('latencies.json', 'r') as f:
    data = json.load(f)

# Convert to pandas DataFrame
df = pd.DataFrame(data)

# Convert timestamp to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Create figure with subplots
fig, axes = plt.subplots(2, 1, figsize=(12, 10))

# Plot 1: Line plot of latency over time
sns.lineplot(data=df, x='timestamp', y='latency_ms', marker='o', ax=axes[0])
axes[0].set_title('MQTT to Kafka Latency Over Time', fontsize=14, fontweight='bold')
axes[0].set_xlabel('Timestamp', fontsize=12)
axes[0].set_ylabel('Latency (ms)', fontsize=12)
axes[0].grid(True, alpha=0.3)

# Add mean line
mean_latency = df['latency_ms'].mean()
axes[0].axhline(y=mean_latency, color='r', linestyle='--', label=f'Mean: {mean_latency:.2f} ms')
axes[0].legend()

# Plot 2: Distribution plot
sns.histplot(data=df, x='latency_ms', bins=15, kde=True, ax=axes[1])
axes[1].set_title('Latency Distribution', fontsize=14, fontweight='bold')
axes[1].set_xlabel('Latency (ms)', fontsize=12)
axes[1].set_ylabel('Frequency', fontsize=12)

# Add statistics text
stats_text = f'Min: {df["latency_ms"].min()} ms\nMax: {df["latency_ms"].max()} ms\nMean: {mean_latency:.2f} ms\nMedian: {df["latency_ms"].median()} ms'
axes[1].text(0.95, 0.95, stats_text, transform=axes[1].transAxes,
             fontsize=10, verticalalignment='top', horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

plt.tight_layout()
plt.savefig('latency_graph.png', dpi=300, bbox_inches='tight')
print(f"Graph saved as 'latency_graph.png'")
print(f"\nLatency Statistics:")
print(f"  Mean: {mean_latency:.2f} ms")
print(f"  Median: {df['latency_ms'].median()} ms")
print(f"  Min: {df['latency_ms'].min()} ms")
print(f"  Max: {df['latency_ms'].max()} ms")
print(f"  Std Dev: {df['latency_ms'].std():.2f} ms")

plt.show()