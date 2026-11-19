# 15. Legal and ethical implications of combining the datasets

*Draft, Mohamed, 19 Nov 2026. Numbering follows the master report (Part 1 = chapters 1–6, Part 2 = chapters 7–17). Figures from the first scorer export (60,000 October games); update after the full-data run.*

*[Co-written with Fede: Fede to add the label-collection specifics in 15.2 and check 15.4 against the final model results.]*

Each dataset on its own carries limited risk. Combined, they produce a behavioural score attached to a pseudonymous but traceable person, and the main legal and ethical risks come from that combination rather than from any single source.

## 15.1 Licensing and terms of use

The game records remain under the Creative Commons CC0 licence of the Lichess open database, so processing three full months on the cluster needs no permission. The account data still comes from the public Lichess API, so the collector at scale follows the same rules as in Part 1: one request at a time, a descriptive User-Agent, a pause after any rate-limit response, and no repeated requests for profiles already stored. Neither source restricts academic use, but neither grants a right to publish judgements about individual players.

## 15.2 Personal data and pseudonymisation

Usernames, account status and the new risk scores are all personal data under the GDPR. Part 2 keeps the Part 1 safeguards and adds three new ones:

1. **Usernames stay on the cluster.** Only the label collector and the parser read them, in the restricted area. Everything that leaves the cluster carries the 16-character hashed `player_id` only.
2. **Game IDs are treated as identifiers.** A Lichess game ID is public: anyone can open the game page and read both usernames. A table of game IDs and risk scores is therefore as identifying as a table of usernames. The serving database stores a keyed hash of the game ID instead (section 16.2), and no game ID appears in any table, figure or screenshot of this report.
3. **The serving layer is checked before every export.** A privacy check refuses any column that could identify a player or a game, and runs before each database dump.

Pseudonymisation lowers the risk but does not remove it, because anyone with the hashing key, or with enough context such as rating, time control and the exact time of a game, could still single out a player. The data therefore stays personal data, and our legal basis remains legitimate interest for academic research with the safeguards of Article 89.

*[Fede: add the scale of label collection (requests, players, duration) and any API incidents.]*

## 15.3 Profiling and automated decisions

A risk score that evaluates a person's behaviour is profiling under Article 4(4) of the GDPR. Article 22 gives people the right not to be subject to a decision based solely on automated processing that significantly affects them, and closing an account for cheating is such a decision. Our system is designed as decision support only: it orders games for a human reviewer and never closes an account by itself. A production deployment would also need a data protection impact assessment (Article 35), because it would profile a large number of people systematically.

## 15.4 The risk of false accusations

The largest ethical risk is wrongly labelling an honest player. Two facts make this likely if the scores are misused. First, flagged accounts are rare: 0.82% of players in the Part 1 sample. When a condition is that rare, even an accurate model produces many false alarms for every real case. Second, the score measures unusual timing, not engine use, and some honest players have unusual timing: very strong players, players on unstable connections, and players who premove. Section 16.4 shows that the share of high scores already varies by rating band and time control.

We therefore apply three rules throughout the report and the dashboard. We say "flagged" or "high risk", never "cheater". Every score is shown with its confidence. And the dashboard states that a score is a reason to review a game, not evidence of cheating.
