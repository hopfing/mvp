"""Match-grain forward-selection proxy for line markets.

Discovers match-level features for total/spread/player-games line markets by
training binary classifiers per line directly against realized outcomes.
Bypasses the IID chain at discovery time; the chain remains the production
projector. The selected feature set feeds back into a score-state chain
config for inference.
"""
