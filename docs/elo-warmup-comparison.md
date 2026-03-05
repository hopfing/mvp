# Elo Warm-Up Comparison

Hypothesis: Elo calibrated on incomplete pre-2010 data may hurt ratings in the
training window (2020+). Compare model performance across different Elo start dates.

Model: `cmp_3elo_age_2020` (logistic, features: elo_surface_diff, svc_elo_diff, ret_elo_diff, age_diff)
Training window: 2020-01-01 to 2025-12-31, walk-forward 5 splits

## Baseline: Elo from 1947 (all data, unfiltered)

MLflow run: 71101e62248d424bb6c3849462ed19f4

           Accuracy        AUC   Log Loss
Train         66.1%      0.727      0.607
Test          65.9%      0.725      0.609

Segments by Circuit:

  CHAL  65.8% acc | 0.724 AUC | 0.610 ll | 0.8% cal | 14.2% err80 | n=34,646
    surface:
      Carpet   57.1% | 0.659 | 0.654 | 14.6% | 33.3% | n=98
      Clay     65.8% | 0.728 | 0.606 | 1.2% | 13.5% | n=17,032
      Grass    58.7% | 0.651 | 0.654 | 8.2% | 7.7% | n=450
      Hard     65.9% | 0.721 | 0.612 | 0.7% | 14.9% | n=17,066
    round:
      Q1         73.2% | 0.813 | 0.530 | 2.8% | 10.2% | n=8,179
      Q2         65.1% | 0.706 | 0.626 | 2.4% | 19.6% | n=4,205
      R32        63.9% | 0.698 | 0.629 | 2.1% | 17.4% | n=11,478
      R16        63.4% | 0.690 | 0.635 | 2.0% | 17.0% | n=5,720
      QF         63.7% | 0.687 | 0.640 | 2.6% | 21.1% | n=2,896
      SF         56.4% | 0.616 | 0.673 | 5.6% | 21.7% | n=1,444
      F          59.6% | 0.649 | 0.657 | 3.1% | 0.0% | n=722
    betting group:
      Strong     73.2% | 0.813 | 0.530 | 2.8% | 10.2% | n=8,179
      Mid        64.0% | 0.696 | 0.631 | 1.2% | 17.8% | n=24,299
      Tight      57.4% | 0.626 | 0.668 | 4.2% | 17.9% | n=2,166

  TOUR  66.2% acc | 0.729 AUC | 0.606 ll | 1.0% cal | 13.9% err80 | n=15,354
    surface:
      Clay     65.2% | 0.720 | 0.613 | 2.6% | 15.1% | n=5,312
      Grass    67.9% | 0.746 | 0.594 | 2.9% | 9.2% | n=1,216
      Hard     66.5% | 0.732 | 0.604 | 1.0% | 13.7% | n=8,826
    round:
      Q1         67.0% | 0.742 | 0.597 | 2.8% | 15.1% | n=3,144
      Q2         62.7% | 0.687 | 0.633 | 2.4% | 4.5% | n=1,580
      Q3         54.5% | 0.603 | 0.678 | 8.9% | 40.0% | n=224
      RR         59.4% | 0.688 | 0.641 | 10.7% | 15.8% | n=192
      R128       69.8% | 0.764 | 0.575 | 5.0% | 5.5% | n=1,432
      R64        67.8% | 0.742 | 0.598 | 4.5% | 19.1% | n=1,408
      R32        65.7% | 0.728 | 0.607 | 1.6% | 13.9% | n=3,536
      R16        67.5% | 0.731 | 0.606 | 2.9% | 13.2% | n=2,016
      QF         67.9% | 0.732 | 0.613 | 4.0% | 23.5% | n=1,008
      SF         62.0% | 0.710 | 0.615 | 8.0% | 8.3% | n=534
      F          63.4% | 0.679 | 0.649 | 12.0% | 20.0% | n=268
    betting group:
      Qualifying 65.1% | 0.721 | 0.612 | 2.1% | 14.0% | n=4,948
      Main Draw  67.0% | 0.735 | 0.602 | 1.0% | 13.8% | n=9,934
      Final      61.7% | 0.677 | 0.646 | 7.9% | 16.7% | n=460

Calibration (0.7% mean error):
  50%-55%  pred=52.5%  actual=51.4%  err=1.1%  n=4,461
  55%-60%  pred=57.5%  actual=55.8%  err=1.7%  n=4,336
  60%-65%  pred=62.5%  actual=62.3%  err=0.2%  n=4,045
  65%-70%  pred=67.4%  actual=67.5%  err=0.1%  n=3,407
  70%-75%  pred=72.5%  actual=72.5%  err=0.1%  n=2,968
  75%-80%  pred=77.4%  actual=77.7%  err=0.3%  n=2,194
  80%-85%  pred=82.3%  actual=81.6%  err=0.7%  n=1,740
  85%-90%  pred=87.3%  actual=87.1%  err=0.2%  n=1,152
  90%-95%  pred=92.0%  actual=93.7%  err=1.7%  n=588
  95%-100%  pred=96.3%  actual=99.1%  err=2.8%  n=116

High-conf errors: 14.1% of 3,596 predictions at 80%+ were wrong
Temporal drift: +/-0.5% from average

### Top 25 Elo Snapshots (baseline, 1947)

#### Overall Elo
|  # | Player                        |   Elo | Last Match |
|----|-------------------------------|------:|------------|
|  1 | Carlos Alcaraz                | 2646.0 | 2026-02-21 |
|  2 | Jannik Sinner                 | 2558.7 | 2026-02-19 |
|  3 | Novak Djokovic                | 2513.9 | 2026-02-01 |
|  4 | Roger Federer                 | 2399.7 | 2021-07-08 |
|  5 | Alex de Minaur                | 2384.2 | 2026-02-25 |
|  6 | Lorenzo Musetti               | 2359.0 | 2026-01-29 |
|  7 | Alexander Bublik              | 2335.3 | 2026-02-25 |
|  8 | Alexander Zverev              | 2305.1 | 2026-02-25 |
|  9 | Felix Auger-Aliassime         | 2304.8 | 2026-02-27 |
| 10 | Juan Martin del Potro         | 2296.9 | 2022-02-09 |
| 11 | Jack Draper                   | 2292.7 | 2026-02-25 |
| 12 | Robin Soderling               | 2290.6 | 2011-07-17 |
| 13 | Arthur Fils                   | 2270.9 | 2026-02-21 |
| 14 | Daniil Medvedev               | 2265.9 | 2026-02-28 |
| 15 | Rafael Nadal                  | 2252.7 | 2024-07-28 |
| 16 | Tommy Paul                    | 2250.9 | 2026-02-22 |
| 17 | Taylor Fritz                  | 2248.6 | 2026-02-20 |
| 18 | Andrey Rublev                 | 2229.6 | 2026-02-27 |
| 19 | Casper Ruud                   | 2217.0 | 2026-02-25 |
| 20 | Ben Shelton                   | 2209.8 | 2026-02-15 |
| 21 | Holger Rune                   | 2208.3 | 2025-10-18 |
| 22 | Nick Kyrgios                  | 2205.4 | 2026-01-06 |
| 23 | Milos Raonic                  | 2195.8 | 2024-07-27 |
| 24 | Sebastian Korda               | 2194.8 | 2026-03-06 |
| 25 | Francisco Cerundolo           | 2194.6 | 2026-02-28 |

#### Serve Elo (BROKEN — obscure players dominate)
|  # | Player                        |   Elo | Last Match |
|----|-------------------------------|------:|------------|
|  1 | Chris Guccione                | 2105.8 | 2015-10-19 |
|  2 | Fritz Wolmarans               | 2097.7 | 2015-10-19 |
|  3 | Michael Ryderstedt            | 2087.7 | 2012-08-22 |
|  4 | Jannik Sinner                 | 2063.6 | 2026-02-19 |
|  5 | Milos Raonic                  | 2062.8 | 2024-07-27 |
|  6 | Vishnu Vardhan                | 2061.2 | 2026-01-29 |
|  7 | Ivo Karlovic                  | 2060.3 | 2021-10-08 |
|  8 | Carsten Ball                  | 2035.7 | 2015-11-10 |
|  9 | Tobias Simon                  | 2027.1 | 2022-04-11 |
| 10 | Gabriel Trujillo-Soler        | 2024.8 | 2013-09-23 |
| 11 | Peter Wessels                 | 2023.5 | 2009-07-13 |
| 12 | Andriej Kapas                 | 2017.5 | 2017-11-06 |
| 13 | John Isner                    | 2017.1 | 2023-09-03 |
| 14 | Romain Jouan                  | 2009.6 | 2017-10-24 |
| 15 | Felix Auger-Aliassime         | 2009.2 | 2026-02-27 |
| 16 | Kevin Anderson                | 2005.6 | 2023-08-26 |
| 17 | Yuri Schukin                  | 2004.1 | 2013-07-15 |
| 18 | Michael Llodra                | 2000.2 | 2014-11-06 |
| 19 | Konstantin Kravchuk           | 1999.7 | 2022-11-04 |
| 20 | Casper Ruud                   | 1998.8 | 2026-02-25 |
| 21 | Albano Olivetti               | 1998.7 | 2025-09-08 |
| 22 | Elmar Ejupovic                | 1998.0 | 2025-11-09 |
| 23 | Tim van Rijthoven             | 1992.9 | 2025-05-22 |
| 24 | Francesco Forti               | 1988.1 | 2026-02-24 |
| 25 | Reilly Opelka                 | 1987.7 | 2026-03-04 |

#### Return Elo (BROKEN — obscure players dominate)
|  # | Player                        |   Elo | Last Match |
|----|-------------------------------|------:|------------|
|  1 | Andrei Gorban                 | 2169.7 | 2025-05-26 |
|  2 | Dmitri Sitak                  | 2160.0 | 2014-01-27 |
|  3 | Timo Nieminen                 | 2150.3 | 2013-07-25 |
|  4 | Deniss Pavlovs                | 2114.4 | 2012-08-09 |
|  5 | Alejandro Fabbri              | 2113.0 | 2012-05-08 |
|  6 | Guillermo Rivera-Aranguiz     | 2099.3 | 2017-03-07 |
|  7 | Cristhian Ignacio Benedetti   | 2097.1 | 2012-07-10 |
|  8 | James Lemke                   | 2096.0 | 2014-04-09 |
|  9 | Alexander Satschko            | 2093.7 | 2013-11-13 |
| 10 | Sarvar Ikramov                | 2089.7 | 2015-10-13 |
| 11 | Rodrigo Perez                 | 2070.5 | 2013-06-19 |
| 12 | Dusan Lojda                   | 2068.3 | 2016-07-11 |
| 13 | Catalin-Ionut Gard            | 2060.1 | 2017-07-24 |
| 14 | Alessio Di Mauro              | 2056.6 | 2014-08-04 |
| 15 | Mico Santiago                 | 2044.4 | 2017-02-20 |
| 16 | Finn Tearney                  | 2035.7 | 2020-01-01 |
| 17 | Lovro Zovko                   | 2033.1 | 2012-11-06 |
| 18 | Philipp Davydenko             | 2028.9 | 2018-07-23 |
| 19 | Tomas Lipovsek Puches         | 2027.6 | 2023-11-06 |
| 20 | Grzegorz Panfil               | 2026.8 | 2018-09-10 |
| 21 | Franco Roncadelli             | 2025.4 | 2026-03-05 |
| 22 | Miliaan Niesten               | 2022.1 | 2021-03-01 |
| 23 | Enrico Burzi                  | 2022.1 | 2018-02-12 |
| 24 | Ricardo Siggia                | 2014.2 | 2013-09-30 |
| 25 | Cecil Mamiit                  | 2004.3 | 2012-03-14 |

## Test: Elo from 2020+

MLflow run: 1f626514de924b01a049e4c3b1bc0b37
Elo computed on 6,384 players across 162,848 matches (vs 23,570 / 713,893 baseline)

           Accuracy        AUC   Log Loss
Train         66.0%      0.725      0.609
Test          65.9%      0.724      0.610

Segments by Circuit:

  CHAL  65.7% acc | 0.722 AUC | 0.612 ll | 0.8% cal | 14.0% err80 | n=34,646
    surface:
      Carpet   51.0% | 0.618 | 0.672 | 24.7% | 25.0% | n=98
      Clay     65.8% | 0.726 | 0.609 | 1.5% | 13.5% | n=17,032
      Grass    59.1% | 0.645 | 0.660 | 6.9% | 17.6% | n=450
      Hard     65.8% | 0.720 | 0.614 | 0.8% | 14.3% | n=17,066
    round:
      Q1         72.8% | 0.809 | 0.537 | 3.6% | 10.4% | n=8,179
      Q2         64.8% | 0.704 | 0.628 | 1.8% | 18.2% | n=4,205
      R32        63.9% | 0.696 | 0.630 | 1.5% | 16.7% | n=11,478
      R16        63.8% | 0.688 | 0.636 | 2.3% | 17.6% | n=5,720
      QF         63.5% | 0.686 | 0.641 | 2.7% | 21.8% | n=2,896
      SF         56.1% | 0.614 | 0.672 | 6.5% | 16.7% | n=1,444
      F          61.2% | 0.652 | 0.658 | 3.7% | 0.0% | n=722
    betting group:
      Strong     72.8% | 0.809 | 0.537 | 3.6% | 10.4% | n=8,179
      Mid        64.0% | 0.694 | 0.632 | 0.6% | 17.4% | n=24,299
      Tight      57.8% | 0.625 | 0.668 | 3.5% | 15.0% | n=2,166

  TOUR  66.5% acc | 0.729 AUC | 0.607 ll | 1.1% cal | 11.9% err80 | n=15,354
    surface:
      Clay     65.7% | 0.720 | 0.614 | 2.3% | 12.6% | n=5,312
      Grass    68.3% | 0.758 | 0.586 | 3.3% | 9.4% | n=1,216
      Hard     66.8% | 0.731 | 0.605 | 1.5% | 12.0% | n=8,826
    round:
      Q1         68.2% | 0.741 | 0.598 | 2.5% | 13.9% | n=3,144
      Q2         62.8% | 0.691 | 0.632 | 1.9% | 5.6% | n=1,580
      Q3         56.2% | 0.609 | 0.675 | 11.8% | 0.0% | n=224
      RR         60.4% | 0.680 | 0.642 | 9.3% | 11.1% | n=192
      R128       70.8% | 0.770 | 0.575 | 5.2% | 7.3% | n=1,432
      R64        68.3% | 0.745 | 0.597 | 3.6% | 16.2% | n=1,408
      R32        65.6% | 0.726 | 0.607 | 2.2% | 11.9% | n=3,536
      R16        67.5% | 0.731 | 0.605 | 4.1% | 9.0% | n=2,016
      QF         67.1% | 0.728 | 0.616 | 3.9% | 20.9% | n=1,008
      SF         62.0% | 0.705 | 0.619 | 7.3% | 5.3% | n=534
      F          60.4% | 0.692 | 0.637 | 9.6% | 33.3% | n=268
    betting group:
      Qualifying 65.9% | 0.721 | 0.613 | 1.3% | 12.5% | n=4,948
      Main Draw  67.1% | 0.735 | 0.602 | 1.1% | 11.7% | n=9,934
      Final      60.4% | 0.683 | 0.639 | 8.2% | 14.3% | n=460

Calibration (0.8% mean error):
  50%-55%  pred=52.5%  actual=51.8%  err=0.7%  n=4,765
  55%-60%  pred=57.5%  actual=57.0%  err=0.5%  n=4,547
  60%-65%  pred=62.4%  actual=63.4%  err=0.9%  n=4,187
  65%-70%  pred=67.4%  actual=68.1%  err=0.7%  n=3,534
  70%-75%  pred=72.4%  actual=73.6%  err=1.1%  n=2,798
  75%-80%  pred=77.4%  actual=78.7%  err=1.4%  n=2,202
  80%-85%  pred=82.4%  actual=83.1%  err=0.7%  n=1,520
  85%-90%  pred=87.2%  actual=88.3%  err=1.0%  n=962
  90%-95%  pred=92.1%  actual=94.2%  err=2.1%  n=430
  95%-100%  pred=96.3%  actual=96.7%  err=0.4%  n=61

High-conf errors: 13.4% of 2,973 predictions at 80%+ were wrong
Temporal drift: +/-0.8% from average

### Summary: Baseline vs 2020+

| Metric | Baseline (1947) | 2020+ Elo | Delta |
|--------|----------------|-----------|-------|
| Test Accuracy | 65.9% | 65.9% | -- |
| Test AUC | 0.725 | 0.724 | -0.001 |
| Test Log Loss | 0.609 | 0.610 | +0.001 |
| Calibration | 0.7% | 0.8% | +0.1% |
| High-conf errors | 14.1% (n=3596) | 13.4% (n=2973) | -0.7% |
| TOUR err80 | 13.9% | 11.9% | -2.0% |
| CHAL err80 | 14.2% | 14.0% | -0.2% |

Headline metrics nearly identical. High-confidence error rate improved, especially Tour.
Fewer predictions reached 80%+ confidence (2,973 vs 3,596) — less Elo spread without warm-up.

### Top 25 Elo Snapshots (2020+)

#### Overall Elo
|  # | Player                        |   Elo | Last Match |
|----|-------------------------------|------:|------------|
|  1 | Jannik Sinner                 | 2615.7 | 2026-02-19 |
|  2 | Carlos Alcaraz                | 2615.2 | 2026-02-21 |
|  3 | Novak Djokovic                | 2561.3 | 2026-02-01 |
|  4 | Alex de Minaur                | 2443.6 | 2026-02-25 |
|  5 | Alexander Bublik              | 2411.9 | 2026-02-25 |
|  6 | Roger Federer                 | 2396.4 | 2021-07-08 |
|  7 | Alexander Zverev              | 2383.5 | 2026-02-25 |
|  8 | Felix Auger-Aliassime         | 2375.7 | 2026-02-27 |
|  9 | Daniil Medvedev               | 2358.6 | 2026-02-28 |
| 10 | Rafael Nadal                  | 2348.8 | 2024-07-28 |
| 11 | Lorenzo Musetti               | 2347.3 | 2026-01-29 |
| 12 | Taylor Fritz                  | 2319.0 | 2026-02-20 |
| 13 | Andrey Rublev                 | 2307.4 | 2026-02-27 |
| 14 | Tommy Paul                    | 2299.1 | 2026-02-22 |
| 15 | Casper Ruud                   | 2286.8 | 2026-02-25 |
| 16 | Nick Kyrgios                  | 2283.2 | 2026-01-06 |
| 17 | Jack Draper                   | 2280.7 | 2026-02-25 |
| 18 | Milos Raonic                  | 2246.9 | 2024-07-27 |
| 19 | Matteo Berrettini             | 2236.0 | 2026-03-04 |
| 20 | Hubert Hurkacz                | 2234.2 | 2026-03-05 |
| 21 | Frances Tiafoe                | 2229.8 | 2026-02-28 |
| 22 | Alejandro Davidovich Fokina   | 2221.0 | 2026-02-25 |
| 23 | Sebastian Korda               | 2217.9 | 2026-03-06 |
| 24 | Stefanos Tsitsipas            | 2216.4 | 2026-03-05 |
| 25 | Jaume Munar                   | 2211.4 | 2026-02-13 |

Notable changes vs baseline: Sinner overtakes Alcaraz (#2 → #1). Nadal rises #15 → #10.
Medvedev #14 → #9. Tsitsipas #43 → #24. Robin Soderling and del Potro drop out (no 2020+ data warm-up).

#### Serve Elo (FIXED — credible leaderboard)
|  # | Player                        |   Elo | Last Match |
|----|-------------------------------|------:|------------|
|  1 | Jannik Sinner                 | 2063.6 | 2026-02-19 |
|  2 | Milos Raonic                  | 2053.7 | 2024-07-27 |
|  3 | John Isner                    | 2016.5 | 2023-09-03 |
|  4 | Felix Auger-Aliassime         | 2009.2 | 2026-02-27 |
|  5 | Casper Ruud                   | 1998.8 | 2026-02-25 |
|  6 | Elmar Ejupovic                | 1998.0 | 2025-11-09 |
|  7 | Kevin Anderson                | 1995.4 | 2023-08-26 |
|  8 | Tim van Rijthoven             | 1992.6 | 2025-05-22 |
|  9 | Francesco Forti               | 1987.7 | 2026-02-24 |
| 10 | Reilly Opelka                 | 1987.7 | 2026-03-04 |
| 11 | Giovanni Mpetshi Perricard    | 1976.5 | 2026-03-05 |
| 12 | Gabriel Diallo                | 1973.8 | 2026-03-04 |
| 13 | Antoine Bellier               | 1971.6 | 2024-10-09 |
| 14 | Benjamin Lock                 | 1960.0 | 2024-11-27 |
| 15 | Martin Damm                   | 1959.9 | 2026-03-04 |
| 16 | Matteo Berrettini             | 1950.9 | 2026-03-04 |
| 17 | Francesco Passaro             | 1949.2 | 2026-02-25 |
| 18 | Stefano Napolitano            | 1943.8 | 2026-03-08 |
| 19 | Jack Draper                   | 1943.5 | 2026-02-25 |
| 20 | Makoto Ochi                   | 1939.9 | 2026-01-26 |
| 21 | Juan Pablo Paz                | 1939.5 | 2025-07-30 |
| 22 | Nick Kyrgios                  | 1938.2 | 2026-01-06 |
| 23 | Brandon Nakashima             | 1936.3 | 2026-02-27 |
| 24 | Hubert Hurkacz                | 1935.8 | 2026-03-05 |
| 25 | Ernesto Escobedo              | 1935.2 | 2025-02-06 |

Guccione/Wolmarans/Ryderstedt gone. Sinner/Raonic/Isner now top 3. MPP, Opelka,
Berrettini all present. Much more credible.

#### Return Elo (still noisy, but improved)
|  # | Player                        |   Elo | Last Match |
|----|-------------------------------|------:|------------|
|  1 | Franco Roncadelli             | 2025.4 | 2026-03-05 |
|  2 | Eric Vanshelboim              | 2002.3 | 2026-02-17 |
|  3 | Gabriel Decamps               | 2000.1 | 2024-07-10 |
|  4 | Jose Pereira                  | 1995.1 | 2026-02-23 |
|  5 | Gilbert Klier Junior          | 1981.0 | 2025-06-09 |
|  6 | Max Alcala Gurri              | 1976.8 | 2026-01-25 |
|  7 | Filippo Moroni                | 1957.7 | 2025-12-08 |
|  8 | Santiago De La Fuente         | 1954.4 | 2026-03-02 |
|  9 | Johan Nikles                  | 1952.3 | 2026-02-23 |
| 10 | Harry Wendelken               | 1951.2 | 2026-03-07 |
| 11 | Javier Barranco Cosano        | 1944.1 | 2026-03-04 |
| 12 | Carlos Gimeno Valero          | 1942.8 | 2025-06-09 |
| 13 | Ryan Peniston                 | 1942.5 | 2026-02-11 |
| 14 | Damien Wenger                 | 1940.4 | 2025-10-22 |
| 15 | Hyeon Chung                   | 1939.4 | 2026-02-23 |
| 16 | Juan Bautista Otegui          | 1939.0 | 2026-01-13 |
| 17 | Ivan Marrero Curbelo          | 1931.8 | 2026-03-03 |
| 18 | Edoardo Lavagno               | 1929.5 | 2024-12-16 |
| 19 | Guido Ivan Justo              | 1926.2 | 2026-02-26 |
| 20 | Diego Augusto Barreto Sanchez | 1924.5 | 2026-02-04 |
| 21 | Ignacio Monzon                | 1924.2 | 2026-02-05 |
| 22 | Ignacio Carou                 | 1923.6 | 2025-11-17 |
| 23 | Calum Puttergill              | 1923.5 | 2025-12-08 |
| 24 | Matija Pecotic                | 1917.6 | 2025-05-21 |
| 25 | Juan Bautista Torres          | 1916.5 | 2026-02-27 |

Still dominated by obscure players — return Elo needs more stat coverage to stabilize.
Scale compressed (top was 2170 baseline → 2025 now). Likely a fundamental data sparsity
issue with return stats rather than a warm-up problem.

---

## New Elo: Opponent-Relative Serve/Return (2026-03-05 redesign)

Serve/return Elo replaced from EMA (opponent-agnostic) to true opponent-relative
Elo using logistic expected score. See `docs/plans/2026-03-05-serve-return-elo-redesign.md`.

Each match produces two sub-games (A serves vs B returns, B serves vs A returns).
Score normalized: `clamp((serve_pct - surface_baseline) / 0.20 + 0.5, 0, 1)`.
Zero-sum: server gain = returner loss.

### New Elo: Alltime (unfiltered)

MLflow run: d00bf846

           Accuracy        AUC   Log Loss
Train         66.1%      0.727      0.607
Test          66.0%      0.725      0.609

Calibration: 0.9% mean error
High-conf errors: 14.2% of predictions at 80%+ were wrong
TOUR err80: 14.1%

### New Elo: 2020+

MLflow run: 4162324d

           Accuracy        AUC   Log Loss
Train         66.1%      0.726      0.608
Test          66.0%      0.724      0.610

Calibration: 0.8% mean error
High-conf errors: 13.4% of predictions at 80%+ were wrong
TOUR err80: 12.2%

### New Elo: 2015+

MLflow run: a3c992f5

           Accuracy        AUC   Log Loss
Train         66.1%      0.727      0.607
Test          65.9%      0.725      0.609

Calibration: 0.8% mean error
High-conf errors: 14.0% of predictions at 80%+ were wrong
TOUR err80: 13.8%

### New Elo: 2010+

MLflow run: c21724e9

           Accuracy        AUC   Log Loss
Train         66.0%      0.727      0.607
Test          65.9%      0.725      0.609

Calibration: 0.9% mean error
High-conf errors: 14.1% of predictions at 80%+ were wrong
TOUR err80: 13.9%

### Summary: New Elo across cutoffs

| Config | Acc | AUC | LL | Cal | err80 | TOUR err80 |
|--------|-----|-----|----|-----|-------|------------|
| Alltime | 66.0% | 0.725 | 0.609 | 0.9% | 14.2% | 14.1% |
| 2020+ | 66.0% | 0.724 | 0.610 | 0.8% | 13.4% | 12.2% |
| 2015+ | 65.9% | 0.725 | 0.609 | 0.8% | 14.0% | 13.8% |
| 2010+ | 65.9% | 0.725 | 0.609 | 0.9% | 14.1% | 13.9% |

Headline metrics essentially identical across all cutoffs. 2020+ has best
high-confidence error rate (13.4% overall, 12.2% Tour). Date cutoff matters
more for high-conf accuracy than for headline metrics.

### Summary: Old EMA vs New Elo (alltime)

| Metric | Old EMA | New Elo | Delta |
|--------|---------|---------|-------|
| Test Accuracy | 65.9% | 66.0% | +0.1% |
| Test AUC | 0.725 | 0.725 | -- |
| Test Log Loss | 0.609 | 0.609 | -- |
| Calibration | 0.7% | 0.9% | +0.2% |
| High-conf errors | 14.1% | 14.2% | +0.1% |

Headline metrics unchanged — the old EMA happened to produce similar predictive
signal despite being methodologically wrong. The real win is credible leaderboards
and correct opponent-relative updates (both players updated per match).

### Top 25 Serve/Return Elo Snapshots (New Elo, alltime)

#### Serve Elo (NEW — opponent-relative)
|  # | Player                        |   Elo | Last Match |
|----|-------------------------------|------:|------------|
|  1 | Jannik Sinner                 | 1751.2 | 2026-02-19 |
|  2 | Roger Federer                 | 1705.9 | 2021-07-08 |
|  3 | Milos Raonic                  | 1699.3 | 2024-07-27 |
|  4 | Ivo Karlovic                  | 1694.1 | 2021-10-08 |
|  5 | John Isner                    | 1685.2 | 2023-09-03 |
|  6 | Carlos Alcaraz                | 1676.3 | 2026-02-21 |
|  7 | Felix Auger-Aliassime         | 1668.0 | 2026-02-27 |
|  8 | Novak Djokovic                | 1659.1 | 2026-02-01 |
|  9 | Giovanni Mpetshi Perricard    | 1657.8 | 2026-03-05 |
| 10 | Nick Kyrgios                  | 1650.9 | 2026-01-06 |

#### Return Elo (NEW — opponent-relative)
|  # | Player                        |   Elo | Last Match |
|----|-------------------------------|------:|------------|
|  1 | Carlos Alcaraz                | 1672.5 | 2026-02-21 |
|  2 | Jannik Sinner                 | 1661.0 | 2026-02-19 |
|  3 | Alex de Minaur                | 1637.3 | 2026-02-25 |
|  4 | Rafael Nadal                  | 1635.1 | 2024-07-28 |
|  5 | Daniil Medvedev               | 1627.5 | 2026-02-28 |
|  6 | Novak Djokovic                | 1621.4 | 2026-02-01 |
|  7 | Roger Federer                 | 1610.6 | 2021-07-08 |
|  8 | Alexander Zverev              | 1603.1 | 2026-02-25 |
|  9 | Lorenzo Musetti               | 1598.2 | 2026-01-29 |
| 10 | Andy Murray                   | 1595.3 | 2024-08-01 |

Serve Elo: Sinner, Federer, Raonic, Karlovic, Isner — all known elite servers.
Return Elo: Alcaraz, Sinner, de Minaur, Nadal, Medvedev — credible returners.
No more obscure players dominating. The redesign fixed the leaderboard problem.
