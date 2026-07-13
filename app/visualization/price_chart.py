"""Price chart helpers for the Streamlit dashboard."""

from datetime import timedelta

import plotly.graph_objects as go
from plotly.subplots import make_subplots

def create_price_chart(df, predicted_price, ticker_name):
    """Create interactive price chart with prediction"""
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(f'{ticker_name} Price & Indicators', 'RSI', 'MACD'),
        row_heights=[0.6, 0.2, 0.2]
    )
    
    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df['timestamp'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='Price'
        ),
        row=1, col=1
    )
    
    # Moving averages
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['SMA_20'], 
                   name='SMA 20', line=dict(color='orange', width=1)),
        row=1, col=1
    )
    
    # Bollinger Bands
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['BB_Upper'], 
                   name='BB Upper', line=dict(color='gray', width=1, dash='dash')),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['BB_Lower'], 
                   name='BB Lower', line=dict(color='gray', width=1, dash='dash'),
                   fill='tonexty', fillcolor='rgba(128,128,128,0.1)'),
        row=1, col=1
    )
    
    # Predicted price point
    if predicted_price:
        last_timestamp = df['timestamp'].iloc[-1]
        next_timestamp = last_timestamp + timedelta(hours=16)  # Next close
        
        fig.add_trace(
            go.Scatter(
                x=[last_timestamp, next_timestamp],
                y=[df['close'].iloc[-1], predicted_price],
                mode='lines+markers',
                name='Prediction',
                line=dict(color='red', width=2, dash='dash'),
                marker=dict(size=10, color='red')
            ),
            row=1, col=1
        )
    
    # RSI
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['RSI'], 
                   name='RSI', line=dict(color='purple', width=1)),
        row=2, col=1
    )
    fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
    
    # MACD (more useful than volume for indices which don't have volume data)
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['MACD'], 
                   name='MACD', line=dict(color='blue', width=1.5)),
        row=3, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['Signal_Line'], 
                   name='Signal', line=dict(color='orange', width=1.5)),
        row=3, col=1
    )
    # MACD Histogram
    macd_hist = df['MACD'] - df['Signal_Line']
    colors = ['green' if val >= 0 else 'red' for val in macd_hist]
    fig.add_trace(
        go.Bar(x=df['timestamp'], y=macd_hist, 
               name='MACD Histogram', marker_color=colors, opacity=0.5),
        row=3, col=1
    )
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=3, col=1)
    
    fig.update_layout(
        height=800,
        showlegend=True,
        xaxis_rangeslider_visible=False,
        hovermode='x unified'
    )
    
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="RSI", row=2, col=1)
    fig.update_yaxes(title_text="MACD", row=3, col=1)
    
    return fig

