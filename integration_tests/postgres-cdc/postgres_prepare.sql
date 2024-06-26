create table person (
  "id" int,
  "name" varchar(64),
  "email_address" varchar(200),
  "credit_card" varchar(200),
  "city" varchar(200),
  PRIMARY KEY ("id")
);

ALTER TABLE
  public.person REPLICA IDENTITY FULL;

CREATE PUBLICATION rw_publication FOR TABLE public.person;

INSERT INTO person VALUES (1000, 'vicky noris', 'yplkvgz@qbxfg.com', '7878 5821 1864 2539', 'cheyenne');
INSERT INTO person VALUES (1001, 'peter white', 'myckhsp@xpmpe.com', '1781 2313 8157 6974', 'boise');
INSERT INTO person VALUES (1002, 'sarah spencer', 'wipvdbm@dkaap.com', '3453 4987 9481 6270', 'los angeles');
INSERT INTO person VALUES (1003, 'vicky jones', 'kedmrpz@xiauh.com', '5536 1959 5460 2096', 'portland');
INSERT INTO person VALUES (1004, 'julie white', 'egpemle@lrhcg.com', '0052 8113 1582 4430', 'seattle');
INSERT INTO person VALUES (1005, 'sarah smith', 'siqjtnt@tjjek.com', '4591 5419 7260 8350', 'los angeles');
INSERT INTO person VALUES (1006, 'walter white', 'fwdbytp@zepzq.com', '1327 3245 1956 8200', 'san francisco');
INSERT INTO person VALUES (1007, 'walter spencer', 'ktncerj@jlikw.com', '5136 7504 2879 7886', 'los angeles');
INSERT INTO person VALUES (1008, 'john abrams', 'jmvgsrq@nyfud.com', '6064 8548 6057 2021', 'redmond');
INSERT INTO person VALUES (1009, 'peter noris', 'bhjbkpk@svzrx.com', '1063 2940 2119 8587', 'cheyenne');
INSERT INTO person VALUES (1010, 'kate smith', 'fvsmqlb@grtho.com', '9474 6887 6463 6972', 'bend');
INSERT INTO person VALUES (1011, 'vicky noris', 'chyakdh@acjjz.com', '9959 4034 5717 6729', 'boise');
INSERT INTO person VALUES (1012, 'walter jones', 'utfqxal@sfxco.com', '8793 6517 3085 0542', 'boise');
INSERT INTO person VALUES (1013, 'sarah walton', 'xdybqki@xrvkt.com', '2280 4209 8743 0735', 'kent');
INSERT INTO person VALUES (1014, 'walter abrams', 'cbujzko@ehffe.com', '1235 3048 6067 9304', 'phoenix');
INSERT INTO person VALUES (1015, 'vicky jones', 'xyygoyf@msejb.com', '3148 5012 3225 2870', 'los angeles');
INSERT INTO person VALUES (1016, 'john walton', 'yzbccmz@hdnvm.com', '0426 2682 6145 8371', 'seattle');
INSERT INTO person VALUES (1017, 'luke jones', 'yozosta@nzewf.com', '9641 9352 0248 2749', 'redmond');
INSERT INTO person VALUES (1018, 'julie white', 'clhqozw@gioov.com', '3622 5461 2365 3624', 'bend');
INSERT INTO person VALUES (1019, 'paul abrams', 'fshovpk@ayoej.com', '4433 7863 9751 7878', 'redmond');
INSERT INTO person VALUES (1020, 'deiter smith', 'nqgdcpx@sumai.com', '0908 3870 4528 4710', 'boise');
INSERT INTO person VALUES (1021, 'john walton', 'zzjwizw@skwdx.com', '2404 5072 3429 2483', 'phoenix');
INSERT INTO person VALUES (1022, 'paul walton', 'zwhnjwb@ojuft.com', '0402 5453 9709 8030', 'portland');
INSERT INTO person VALUES (1023, 'peter bartels', 'gwlteve@aikvf.com', '6555 8884 1360 0295', 'redmond');
INSERT INTO person VALUES (1024, 'saul shultz', 'mghpttp@sxihm.com', '7987 2816 9818 8727', 'cheyenne');
INSERT INTO person VALUES (1025, 'julie bartels', 'cxjfsuu@uwcpw.com', '0352 3457 2885 0266', 'san francisco');
INSERT INTO person VALUES (1026, 'paul spencer', 'plcully@qwfas.com', '2017 1897 0926 6328', 'los angeles');
INSERT INTO person VALUES (1027, 'luke white', 'jtatgee@wjaok.com', '2465 7541 1015 4655', 'portland');
INSERT INTO person VALUES (1028, 'kate white', 'mmcqrfk@fldvr.com', '3696 3808 1329 0692', 'seattle');
INSERT INTO person VALUES (1029, 'kate spencer', 'wkixktk@nqzin.com', '8540 3588 4648 5329', 'portland');
INSERT INTO person VALUES (1030, 'sarah walton', 'bhinrlm@itvuw.com', '1009 7742 8888 9596', 'portland');
INSERT INTO person VALUES (1031, 'luke abrams', 'tmoomlm@umwjm.com', '1161 4093 8361 3851', 'redmond');
INSERT INTO person VALUES (1032, 'saul bartels', 'kkxmkbp@sjldo.com', '5311 2081 6147 8292', 'cheyenne');
INSERT INTO person VALUES (1033, 'sarah smith', 'gixszyd@ikahc.com', '0654 0143 9916 7419', 'cheyenne');
INSERT INTO person VALUES (1034, 'sarah spencer', 'wazwjxh@giysr.com', '8093 7447 4488 2464', 'los angeles');
INSERT INTO person VALUES (1035, 'kate smith', 'xdtubdc@eoqat.com', '1880 7605 7505 3038', 'seattle');
INSERT INTO person VALUES (1036, 'deiter white', 'lzxmcig@pfyrp.com', '8336 1080 3823 2249', 'los angeles');
INSERT INTO person VALUES (1037, 'john jones', 'qdolslh@pzlry.com', '4394 1929 0794 1731', 'los angeles');
INSERT INTO person VALUES (1038, 'walter spencer', 'ljboats@roguq.com', '5990 9981 6050 5247', 'bend');
INSERT INTO person VALUES (1039, 'luke jones', 'sobojsi@vhqkh.com', '1406 2686 9359 7086', 'cheyenne');
INSERT INTO person VALUES (1040, 'luke bartels', 'qtlduro@zijhv.com', '6662 1330 8131 8426', 'cheyenne');
INSERT INTO person VALUES (1041, 'deiter jones', 'chmequx@mkfof.com', '2941 9597 1592 6346', 'phoenix');
INSERT INTO person VALUES (1042, 'john smith', 'odilagg@ckwuo.com', '7919 0755 1682 9068', 'portland');
INSERT INTO person VALUES (1043, 'vicky walton', 'nhcbcvg@kkqvz.com', '0031 6046 4743 7296', 'cheyenne');
INSERT INTO person VALUES (1044, 'peter white', 'bigajpm@tslez.com', '6077 8921 3999 7697', 'bend');
INSERT INTO person VALUES (1045, 'walter shultz', 'vaefysn@unvsg.com', '3638 3193 7385 6193', 'boise');
INSERT INTO person VALUES (1046, 'saul abrams', 'zxfjtbp@fgwli.com', '4031 2701 7554 5688', 'cheyenne');
INSERT INTO person VALUES (1047, 'saul jones', 'xyeymyt@otocr.com', '5732 1968 8707 8446', 'redmond');
INSERT INTO person VALUES (1048, 'peter bartels', 'ysmazaq@rnpky.com', '4696 0667 3826 9971', 'san francisco');
INSERT INTO person VALUES (1049, 'walter noris', 'zeeibrx@aljnm.com', '1484 3392 4739 2098', 'redmond');
INSERT INTO person VALUES (1050, 'peter smith', 'kabfpld@fhfis.com', '5179 0198 7232 1932', 'boise');
INSERT INTO person VALUES (1051, 'julie abrams', 'knmtfvw@lyiyz.com', '3687 0788 3300 6960', 'cheyenne');
INSERT INTO person VALUES (1052, 'peter abrams', 'uweavbw@ijmcd.com', '9341 0308 6833 3448', 'portland');
INSERT INTO person VALUES (1053, 'paul noris', 'hnijvou@zawwc.com', '1502 1867 0969 4737', 'seattle');
INSERT INTO person VALUES (1054, 'sarah jones', 'kmhnjtg@cetsb.com', '3145 3266 2116 5290', 'cheyenne');
INSERT INTO person VALUES (1055, 'kate abrams', 'gyocmgj@uimwr.com', '0552 0064 4476 2409', 'cheyenne');
INSERT INTO person VALUES (1056, 'julie abrams', 'ckmoalu@ndgaj.com', '9479 9270 0678 6846', 'boise');
INSERT INTO person VALUES (1057, 'julie white', 'chxvkez@djjaa.com', '3522 2797 5148 3246', 'cheyenne');
INSERT INTO person VALUES (1058, 'walter abrams', 'rmfqwms@pvttk.com', '8478 3866 5662 6467', 'seattle');
INSERT INTO person VALUES (1059, 'julie spencer', 'nykvghm@kdhpt.com', '9138 9947 8873 7763', 'kent');
INSERT INTO person VALUES (1060, 'kate abrams', 'wqxypwn@jrafo.com', '5422 1018 4333 0049', 'portland');
INSERT INTO person VALUES (1061, 'kate white', 'njkweqw@qlinl.com', '3254 1815 6422 1716', 'san francisco');
INSERT INTO person VALUES (1062, 'luke bartels', 'emoramu@tkqmj.com', '7655 7679 5909 2251', 'portland');
INSERT INTO person VALUES (1063, 'julie spencer', 'acpybcy@fygni.com', '0523 2583 3342 5588', 'portland');
INSERT INTO person VALUES (1064, 'luke spencer', 'rxlzmbi@ftvjh.com', '3989 4985 1721 9240', 'los angeles');
INSERT INTO person VALUES (1065, 'john jones', 'sdjpica@sfddi.com', '7716 1367 0259 3889', 'bend');
INSERT INTO person VALUES (1066, 'paul white', 'gclssac@cjcqr.com', '2708 5518 8447 8022', 'kent');
INSERT INTO person VALUES (1067, 'vicky bartels', 'qsurdwa@zcyxz.com', '9332 8313 3113 1752', 'cheyenne');
INSERT INTO person VALUES (1068, 'john spencer', 'rvdbxjj@thhat.com', '2065 0039 4966 7017', 'phoenix');
INSERT INTO person VALUES (1069, 'luke white', 'rlnjujw@yajij.com', '8511 7005 7854 1288', 'portland');
INSERT INTO person VALUES (1070, 'sarah jones', 'hpuddzw@zqxub.com', '4625 1520 6481 1767', 'bend');
INSERT INTO person VALUES (1071, 'luke shultz', 'uhlejag@whmqq.com', '3427 8456 9076 1714', 'kent');
INSERT INTO person VALUES (1072, 'julie shultz', 'xzwbhur@otviv.com', '6404 5841 0949 2641', 'boise');
INSERT INTO person VALUES (1073, 'vicky walton', 'ercndev@gequo.com', '8807 4321 6973 6085', 'boise');
INSERT INTO person VALUES (1074, 'julie noris', 'jytjumk@fddus.com', '7463 7084 1696 8892', 'kent');
INSERT INTO person VALUES (1075, 'julie bartels', 'hugijat@huhob.com', '4530 8776 7942 5085', 'los angeles');
INSERT INTO person VALUES (1076, 'kate spencer', 'snqygzv@tsnwb.com', '2522 9594 4307 9831', 'boise');
INSERT INTO person VALUES (1077, 'kate jones', 'lsshriy@aknvv.com', '7065 2545 7960 0041', 'portland');
INSERT INTO person VALUES (1078, 'saul walton', 'xveffme@gcplt.com', '5848 5246 7319 1450', 'phoenix');
INSERT INTO person VALUES (1079, 'vicky smith', 'fhcdtoq@aemjt.com', '3071 1822 6864 8221', 'los angeles');
INSERT INTO person VALUES (1080, 'luke shultz', 'zlrbrav@pynxn.com', '2038 4905 4566 6031', 'phoenix');
INSERT INTO person VALUES (1081, 'john shultz', 'giradrs@mavun.com', '3344 8962 5224 8904', 'portland');
INSERT INTO person VALUES (1082, 'john bartels', 'nqxjwrg@ppebb.com', '7144 4781 7168 6500', 'los angeles');
INSERT INTO person VALUES (1083, 'john white', 'kkcnemc@wcdej.com', '6683 7670 7530 0890', 'bend');
INSERT INTO person VALUES (1084, 'walter abrams', 'bmjdpec@ynwal.com', '3594 8838 1244 9650', 'bend');
INSERT INTO person VALUES (1085, 'deiter jones', 'xquhjkv@azyxm.com', '6385 5861 0188 6728', 'los angeles');
INSERT INTO person VALUES (1086, 'vicky shultz', 'lwmmeqx@rvddr.com', '5916 6762 6797 4669', 'los angeles');
INSERT INTO person VALUES (1087, 'vicky walton', 'askxzha@lachv.com', '2178 8782 4988 7051', 'bend');
INSERT INTO person VALUES (1088, 'kate noris', 'tbalnld@nmxkq.com', '3240 6224 1233 7005', 'boise');
INSERT INTO person VALUES (1089, 'vicky noris', 'grjawpy@zkyds.com', '2009 4332 9634 9823', 'boise');
INSERT INTO person VALUES (1090, 'sarah bartels', 'hrpmxnr@rvzgq.com', '0733 1934 0398 7793', 'redmond');
INSERT INTO person VALUES (1091, 'saul walton', 'ntqrfhp@oumoz.com', '8923 8221 6882 0275', 'bend');
INSERT INTO person VALUES (1092, 'paul noris', 'qevgjyo@wubwo.com', '9303 3741 8490 6300', 'portland');
INSERT INTO person VALUES (1093, 'peter white', 'cjbkbke@rtbye.com', '1188 2449 6471 5253', 'boise');
INSERT INTO person VALUES (1094, 'kate smith', 'pbjnaxm@fbgld.com', '3054 4394 5921 6700', 'bend');
INSERT INTO person VALUES (1095, 'luke spencer', 'iamwwkv@cujlu.com', '6643 2101 9195 1615', 'seattle');
INSERT INTO person VALUES (1096, 'luke noris', 'amsxmdf@znzqj.com', '7291 3287 8055 7550', 'kent');
INSERT INTO person VALUES (1097, 'walter abrams', 'djjgtgv@gdhku.com', '9089 0787 4194 7095', 'san francisco');
INSERT INTO person VALUES (1098, 'kate spencer', 'suadlvi@makbh.com', '0823 4419 7875 1675', 'phoenix');
INSERT INTO person VALUES (1099, 'sarah white', 'ynsyxew@rjjmk.com', '4049 9641 0911 0158', 'redmond');
