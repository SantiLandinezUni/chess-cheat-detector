# 17. Conclusions, limitations and further work

*Draft, Mohamed, 19 Nov 2026. Numbering follows the master report (Part 1 = chapters 1–6, Part 2 = chapters 7–17). Figures from the first scorer export (60,000 October games); update after the full-data run.*

*[Editor draft. Everyone: add one or two sentences on your chapter's main result by Wed 25 Nov. Numbers in brackets to be filled from the final runs.]*

## 17.1 Conclusions

Part 2 set out to turn a validated sample into a system that ranks live games for review. Against the seven objectives:

1. **Scale:** [three months processed: N games, N moves; all data-quality checks passed (chapter 8)].
2. **Unlabelled layer:** [baselines for N band × time class cells; anomaly score distribution (chapter 10)].
3. **Labels and model:** [N players labelled; PR-AUC of N against a baseline of N (chapters 11, 12)].
4. **Generalisation:** [performance on held-out months and time controls (chapter 12)].
5. **Real-time scoring:** [score latency p99 and throughput (chapter 13)].
6. **Serving and dashboard:** the serving database and dashboard run from the scorer export with only pseudonymous identifiers, and the database dump passes the privacy and restore checks (chapter 16).
7. **Legal assessment:** the main risks are re-identification through public game IDs and false accusations from profiling; both are addressed by design (chapter 15).

## 17.2 Limitations

- **Labels are proxies.** A terms-of-service flag covers more than engine use, applies to accounts rather than games, and misses undetected cheaters (section 2.5).
- **Timing is not proof.** The scoring rule uses timing only; engine evaluations are still missing for most games.
- **The score is not neutral yet.** High scores are more common in fast time controls and higher rating bands (section 16.4).
- **Three months of one platform.** The generalisation test covers months and time controls, not other platforms.
- [Add limitations from chapters 8–14.]

## 17.3 Further work

- Calibrate the score per rating band and time control so that the share of high scores is comparable across groups.
- Add engine-based features, starting with Stockfish evaluations for games that lack them.
- Export intermediate scores so the dashboard can replay a game with its score updating live.
- Replace the provisional scoring rule with the trained model once it outperforms the baseline.
- Run a data protection impact assessment before any use outside research.
- [Add further work from chapters 8–14.]
