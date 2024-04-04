--AB TEsting Botones
WITH fecha AS (
SELECT
cast('2024-03-24 13:00:00' as timestamp) as fecha_inicio,
cast('2024-03-31 13:00:00' as timestamp) as fecha_fin
),
intents AS (
    SELECT  
        b.ts, 
        b.id, 
        b.session_id, 
        b.message_id, 
        b.message, 
        b.results_intent_id, 
        b.results_intent_name, 
        b.type, 
        b.one_shot, 
        b.threshold_clustering,
        b.results_index 
FROM "caba-piba-consume-zone-db".boti_intent_search_user_buttons b
LEFT JOIN fecha on 1=1
WHERE ts >= fecha_inicio 
    AND ts <= fecha_fin 
    AND b.message != '' 
    AND substr(b.session_id, 1,20) NOT IN ('GE6NEV1ZHNDRXIAAQVGV', 'LU0VWVFLOK130EGKD1LR', 'S2311UDJQVQZ1B4L1LM2', 'D7GIKRV4B6Y7O83KBVXV', 'VNDVGYFJHMVU1GV4LUB3', 'XYRVASSYPE16OBJ8V6DZ', '2EM2NMQYWOPYN0CV8HCD',
 '6VM3Y8KV4JQJT8668SZ7', 'KPLSPQRP7VW5FF7TVD1C', '8HAGJQCQ07OA5CYPX0CU', 'RDK3GZAXQXZ1MOM4TJSR', 'LNN6YM0L0EFJG18PFQ0Q', 'MSO7D4PBPNLMN720N0B5', '3SMHCS76U48K1CYS3COQ', 'LLHY6OKHKKA2H3Q27ECF',
 'CAZ2MCLP5AVOUH6GQCX8', 'OFZNMR7KACRNLDBGA0Z1', 'XEY1GBUW5EPJLC2IG8H8', 'ZSY2HLF0TRQKYSM8WBPS', 'IGM51NE7UG7S3BQH84JF', 'K0YAGG27HY2VVZDWYW56', '86KSSEVOPT0O3AL4CXHG', 'RNI7IGZ02STNMRKTIP0F',
 'MXYPVCSAOTY6E584HXVN', '1T30FJST0Q0OIKJ8JUR5', '5GUFRW0UHKJ28VF15GMI', '5JKTFZ4E5CTN5AQEXC46', 'NW3JOVVXMQE2OPTU86QR', 'COHKGGA3LDTSX302GKK4', '1NXTSI2W7MAYO6G7YG65', 'A5I1066RRAPUXORCYGE5',
 'EDJKQY0IPEDNIY8YXB0H', '10CTW1QNNCZ2FWZTZW2E', 'DMOR4H6C5KW86E5UP6CB', 'RMS1KUNZVH6NQX48NON1', 'STKN8HRZYEQ54UHGSQME', 'DF5U0WKLF77AXQSWW268', 'W7Q2JAMJE2CWHOADXYHP', 'XNJJUZYMF5QPJGBI78J1',
 'WRS3784MONQPAXRLUT8I', '8CA0X0E76WFI2KESTM3D', '7KJ3UGIV2WIDG5VP2C8L', '5FICWKGU5PL3IN4NI55U', '05K4OW153MTK1T3XFFHG', '6Z45ZODLXIENOA5WE8WS', 'Z00CHMDNTW5MX4WIOHCO', 'HRKUP6MHBUMAEJ2X3MEL',
 '54VDTFY7DXXW1UHE2KXR', 'ALCZXKZ585ENEO7P5E1F', 'PFSW8O71LR3V0AKSCI4Q', 'S7CEL4WZ04MAY34GFW4G', 'XKJSSDK0L03GCK64ORQ8', 'RSMUHI2T5Y7Z03UVP08J', 'YNPGOR3PCSQB54XG4XNT', 'IIOTQVZ7QXXHI82PZDFH',
 'UCEDWAYFQ45QAC0MSD73', 'JQVNXUV47DL0O7DOS5A1', 'UTJ3UQGM5SHQRORCRFHM', 'YEAZBDPSTA1LQYJ8FRG8', '3XY0N6BMVO71BCA4BGYI', 'MF3QLCI26MVK7RUBBW26', '4HIO5J3RPDAW6ERA5J1I', '2HCECIKVNSJ1NQ6ML60L',
 '5NV4KL53LA0ZU8DNY8BM', '416CBSNX4CEHQYSCH6W7', '2DM47IM1P4PFO12B1FEQ', 'NUK6B5DO4RFRW76DIQWX', '2DPFLZ4A4PJ3AUJWN60R', 'FW452YBT78Q2WZGLHETY', 'KB4BM5NJG75Z6K8INT8A', 'FYLMD4U7RSSNTRH7NOTQ',
 'P2JXJFKANQEAQ42LQ0WZ', '7W6FDT7Y76BWWIEB0V63', 'KPAKHSZYS312TZWD6BFM', '11EK1SDV7117YJDS016Q', 'FMELG8O4Y14ZSY2DJJEH', 'C3ALTPELD42IM3F6OXG0', 'QTEJVP452RQOHDOYRW4I', '3FIGK01MK2RJI82T8828',
 'ZTT71WAVNENL610CE6EP', 'TWVQ8ZEN0OU4QBUBX67M', 'FZD4YFINF53S55EXHJUM', 'HLRUTDC4KBRQJKPH00HK', 'I07F8XPL2YLYXE04U60O', 'PQLO35CP2HWAD8ABF3BL', '5GRI6OYE58ZY36321WUB', '26S2Q7JJP7O2WWSJ15BO',
 'NBZ8V57VTULH0LZGRV5R', 'OMAJ0T7TWEYJDE0W2WJF', 'DT2II3PUSKADSPJN70IF', 'LHHFW8LLZ4Z8MNCE4HG4', 'RYOAU5BV4QGAU8ZNNK0I', '1ZBLCPIKSJOT7NSLNRLI', '0HEPK8YTHYEPCV3L48PK', 'E4UH48YXKGATH5QCNPIU',
 'ZPE0EHA6TRTILSO30NCZ', 'JB1BYVAG2NDECRBLSGNX', 'I2BXUYECBNDJO2ZBPX0T', 'W1Q3VVMT70O25Z0YFCGC', 'Q8S3UHJ1J1GUF0PKA4QZ', 'TA7CIZKBI232Y287ILCU', 'K5MY5RI8ATFR6C0528HI', 'J6B346L8CZ8B748LANRH',
 'GVDY5UUDWE8JMSVF3RBZ', 'M63AKBSAPCUKRTWGM6CW', 'KIBFVWUG7C0LNT1O7X88', 'CA5DX5GEGKBR0YFAPFB6', 'KQOE8ZSOGN3YYHL7GCT1', 'D7SEH20QBEST6PDFXR4U', 'TK7THEZRU6ZEH7KZMZO7', 'HVWP7BRGGX6VJQUFFIUH',
 'KLTQOFHKSQRASX4YNPGF', 'BIDWEYY3DORY62PO6F04', 'I41OGTJHSYKSPYGYAGSJ', '5U8TXXUCLZKWWOHMRYCQ', 'TTMEFQOXTVQJ323E8WDN', 'O2MWC6YEJO76HLNV6KF0', 'I4STURN76MO0EICPRT8S', 'XI7CB1EQZYVPX7LGTB10',
 '6NFMPC7NDR0GMIOGAEW7', 'UX5N6GHTXK63NOEXH6X3', 'ERLNEKTOTS06T3ML6LWO', 'G0IS7UVUBWKXS7GMCD3P', 'NANYSAIC33P2EGL1IUFN', 'Z4N1BSYQQTLKL85AL3J4', 'RWW4AUTUBGAAMHWHNEOR', '2BZ3SP381ZSYLEG7LFY3',
 '2HMNO0YDH7ITWPPKJ6ES', 'OYVJS5QDX855RMZH1SLP', 'B5AVFA82MKGDL43QTCUS', 'FTW8OXP1KTVJ4LKYX18A', '41KHZCGGUXIUFL8KSNIA', 'AEBHNPT3NCFHQDNX5YNL', 'AMCPECSKOTFLFACDUM3K', 'ZWCW60GRXLCFJLC81VXE',
 'ESJ2LYXTQMHNJ6HYAHSV', 'S201OHVENXDWL3S17H6J', 'J55FDC85RSZRVFI6SOST', 'WQB8H5GM3ZCSKNKEMO2H', 'OATCGM8104P2A7QKX2T3', 'SEVA1B31TNYINVKEA7AY', 'YS6H6XZK88P2GKW17GK7', 'XU3NCX1NCC25L208LY5Q',
 'WBN2J3W4FUHKWTYZWBA5', 'YTTW2U7BO03CZXQBRIKA', 'S0X1F2MCUPMYCHKOFTCN', 'D24WGNRKFZ8OMKRWONLT', '0LRDMVLOKSSSIAROWJKX', 'I27IDZIWLTA3AQNCRUXR', 'XOWLYWVWV3SKZ3800EVA', 'KVNPME23H1LJKUUVTE01',
 'ZAMYNXDW8YXITVNTRMXA', 'XXH5DJ13N7M05Z8ZPK21', '6UEGPS8YIZC0LWAU1EAD', 'Z13MT5B6YX0CY8MGOT67', 'BQRO5JOEYVONU2O17CCZ', 'IE50XE7SLFZUIAEFX7H3', 'KV5J1F25BPWWOVTMZA7C', 'DBSNN355W5BWZG4XFF85',
 'TI1JUEXQPJUTRDXYHBKH', 'ZI3R6FQWU4010GOPKIHL', 'HR87SSCKPG2JY1GM1RCT', 'D0S2SQD6WFZ5DWQ2J05Q', 'DT3ZIFK8C83JQL32MEUH', 'V6Z3NDO6WRIC5WJPSIG7', 'HQ4MNJM3WRFXEZZS8DPX', 'PT75H47GG6XMUCEUA554',
 'W6DFXFZ8UDTQMSUTD7CR', '72RTCK4Q42QSUM6YLEGL', '4QJUIUXR500BPSTQZEYK', 'XFFFA0X1OLBSO3LJSOYW', 'EV7PPXV6PTFWMM41IID3', 'VFZDLWXA7P76M4PYJ04B', '8NUTTHQUIFDRPWCNCV8Y', 'C78XWWKTSXNUN0MWPVB8',
 'BQT56U063YPYHA3FYU0I', 'YUA06EWCDY47J78ATC6W', '0O2JDGCJHI4EZMC7SCM4', 'MZOUAWZD3JDO4HDCC8TN', 'EPFHYXE6VSJKG5Q5C1KX', 'CJHMEZ2SKLAED6SJJD6U', 'PVOTF80XWV8MU1S6IZ8G', 'APDYPDJHDA8YF5HDKZOD',
 '270VLN7DECHO4U3JCUJ0', 'LUZW4J23RGR5866W88BH', 'BLQ2TAQVWQRXIJFASCOQ', '70TPX4MRHY6O16M0L3BK', '8KUIRAGW0NU8D7PYSVFQ', 'S1RZCMYX7WI3VDV1UEWP', '1YHZONY5AFGABVOM2JN1', 'SRG5ZL7DGXT6P4TLPCZ8',
 'A2T3LUPBHGAJ18CGJ7P2', 'HLJZDLX2OCPUAOYVLJQM', '003FJHN3B25DX6JPOXNR', 'AY53YXMNITZSCGTG37CO', 'WL6EB3W12PX3KOGMKR0F', 'DZ087RZRIYMFJ3CTHJJD', 'YKBZLTTLPT8Q242GZXAZ', 'TQ73HSOTGBAHF008HWPR',
 '4NJLPRX75THDT3667EKA', '5JLQART8RSEEDN62WRCB', 'CPZ8KS7MHFAZSY14VSPA', 'LKWV7DMSXCOJ02N2SSSJ', 'U63EN6XDU43708ITGZBA', 'X4AXQTV2AJNFP24UJXZ0', 'TNBR3DDZ166E12MCMZBV', '1GENTRUZHS1SRIDUBMEZ',
 '3EUFPIJV0HFBBQ2ROMWU', 'JPHKHDGNIIC60D6F0VDC', 'BWXA3Z4SXFROSNV0Y5SY', 'D2LK4YJR12JTTYWVJS74', 'M55CKYR460QS3DLBQEPH', 'RN3MMC1881VRJ62PWWOT', 'OXFSN1CMVKM81ASY3QE6', 'W4Q2M1AEXC2RF058HVJR',
 'TCXHKWZQEGBH2A1OOWAO', '121JEQYRMUTIOFAYCBOW', 'LJ0L5W5A72OUPDKV5OGS', 'W871FH5AN12E4EKFFT43', '0IYHNJENWZWPUW6NLEAT', 'DI6HQKRJTKL8BR6XDEMY', 'QB0SZYRYQS5V52GVZLMR', '1JELDRIGGQ8CTO31QAAZ',
 '56N5KWRD0V0CHM3EAY3T', 'L3422VNZJ64STJXC3NEZ', 'H02T4IH3DZBZBHKIAYD1', 'CTC0JBJQW312F1NQ2WKB', 'OBUIMVOR3JKFFR35QPE8', 'SLIGVL0SZS1ME2MTHMLP', '0IBR5P1LF0JBOJ2C1KMX', 'J3VEEN02HN5SLJIWLZXZ',
 'C00I86156RNT8PM7C8LI', '5TYMCGJ5Y6Z0GAZCY1RD', 'E6VL4S6MP4KIP2OHCI68'))
 SELECT *
 FROM intents
 