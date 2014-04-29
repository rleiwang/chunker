package chunker

// randomized cyclic polynomial hash
// http://en.wikipedia.org/wiki/Rolling_hash
// https://github.com/lemire/rollinghashjava/blob/master/src/rollinghash/CyclicHash.java

var (
	// generate 2^8 64 bits random number on Ubuntu
	// dd if=/dev/urandom count=4 2>/dev/null | od -t u8 | awk '{print $2,$3}'
	htbl = [256]uint64{
		12581444695541775663, 18385197646123105605,
		7498815613428088051, 1652026037071615812,
		2868356445306630412, 16770287200473769355,
		14755495822551106192, 10780043515397382507,
		10848587509946941514, 17952216316679681188,
		12237780550586650794, 7037465092561699253,
		8459271907028661872, 6599540457862460823,
		15679269750784574793, 5584805242854784482,
		13477655642126582989, 9033969097276089320,
		4674211979164591411, 14610535043809694018,
		4542735018006653666, 6768292739257008048,
		7123069150166230601, 17904742758522041397,
		9848942285665670420, 6370765447592428796,
		12878478255298300764, 13365492338438574435,
		12664619811813268609, 15824710172478545193,
		12055243063143500352, 13838527088748883328,
		8192684072812484001, 1340816452556561734,
		11532211982444988072, 7549478855577041915,
		252029848685179655, 13286582569383512323,
		8619817146967674952, 10085837132686968912,
		15749339482811791672, 15116967608490861080,
		3856200574325933051, 17109777237171585295,
		1145113740322029776, 17303286499428363165,
		15937423563130306905, 16815133899747984224,
		13804218450282550062, 18132588883798438862,
		12076835709548017079, 9702037632584706344,
		10260178080772858436, 17286365171093071885,
		18036451606096903535, 5005230930493960086,
		1262847226894045479, 16790568408833773167,
		16904350758592407415, 16665405017808721379,
		10273643433529563102, 3395993943037330287,
		9005609754611902348, 7308731268860425110,
		14322401140227975388, 17350663641410768473,
		6206291403765374926, 4004051575003396367,
		11100396358554697322, 1681396140798679165,
		9799157318678503254, 13230122719496228490,
		14776295404783115600, 12218578220791002489,
		4803256606932417744, 10042994945126871142,
		12382672495594726855, 4464188293965401126,
		1384631906047685028, 9697757937503253809,
		9826129674208304940, 14828743662563398665,
		1244751335876869213, 1637630464322462184,
		522094333841845127, 10898248533081604168,
		15553640489339467089, 1033838442436149693,
		7209707356484976930, 10179903267701263082,
		318824247642160654, 11188898741888346233,
		4820300760426635723, 26948740512520633,
		18069428632427548335, 5136026330674214820,
		11825998069763814269, 1646314090430065971,
		580167476149613944, 7016068481107825757,
		5922711551186909678, 8341128380779383906,
		14956340726246888232, 2093947710855861309,
		3577610751228070263, 9372540222004736003,
		13003856609707735032, 1196067526015368125,
		11096954679336971116, 13602084126017790463,
		10310288128892632581, 17679516707621002751,
		16910323150851420328, 137428749564232472,
		16824630778112153428, 1611704489705281458,
		16608363974995619201, 6562969487065245483,
		4022959861898601238, 17023853702484193273,
		13762822703126256834, 8541560648055873040,
		6367980389950726059, 18410172472110097803,
		13843468741795226473, 17958159243090649683,
		9726192323611839355, 15621613524101403081,
		12292829922249563070, 6559598670310571138,
		16175892898043229184, 4015828013364073583,
		13744790460011111177, 6701179618172559482,
		261560594023777566, 17913844806688898465,
		4541241250787175597, 9904867698905765625,
		16995550832346765885, 13730664810820895936,
		8138116142930701380, 2052116071681498677,
		10124059928159865564, 4115494067594543253,
		6353129868213553776, 13275083959131709232,
		15089751907264420808, 15928954226625532810,
		14007047415109604959, 10744062530072615196,
		3546316394751799018, 3895771978398139414,
		15498966486883367642, 7526916811211293954,
		15383522884730741117, 14147922595333072278,
		17198982895168862232, 13956877923643993890,
		18187988221772953656, 16421151947822673594,
		4621007129708966776, 13862845092735425200,
		12627735878725907478, 7953874504186595136,
		12017922778583264230, 5573976238784579172,
		2540897300778523899, 5019495583898630214,
		17267227166828061223, 17481492765698477704,
		5208047688348006696, 2739974984112734822,
		18122740041796745444, 15254004643662495078,
		10503423807648359404, 17489426611811210255,
		2561744722394080750, 6423130779422127870,
		7357750101582679014, 7245798639694301817,
		8687926714838453416, 2784479605869321878,
		9962569219051274158, 4945871781645824100,
		5746282153445240141, 12712276474169752302,
		11889782065350116401, 3268282142868993290,
		16893942486075199966, 11093597784571069083,
		15243218551164820102, 15153003520911680027,
		4122639190270772409, 11037118196270457161,
		12117929246440318664, 2581065691485973277,
		9667892766898125923, 2635479714936550491,
		17672829800670154594, 13425103715283235800,
		13265858519430206296, 9646937829345838996,
		3487305348433726314, 7423062838702526222,
		4618630887834885474, 7289112842372660485,
		5662819915135647334, 15976543425231647581,
		17207614751534815737, 9334714563506264194,
		10477028752873342155, 13621952179289225228,
		13572381912068523596, 10455486701512305184,
		12428706510457948398, 16988759656069780734,
		10718626467696766827, 7048545809690572750,
		18028486026610326536, 17269025909181226219,
		7234196164470075166, 9566765417727670790,
		2699565221981225455, 3356229611075024475,
		6956853414382619550, 6803914228474869298,
		1086331540294493871, 11908731056901605716,
		8278233173496424826, 6065198648304812275,
		16296775936815229336, 18079240246090566371,
		5432343900531922118, 3075139684227911169,
		8951802902121167716, 11322386074005023957,
		17251524823736313459, 7145355666399705106,
		11122670048580226111, 15191324765939129123,
		2609471635435914378, 1116465118213877311,
		15435206699559502078, 3881424720357065805,
		6598395613786716185, 13432055020716384161,
		15031616754082131135, 2238465500880701385,
		6489978019345218742, 10182919324931193056,
		238137675493854981, 5380330328386326146,
		16072066314910980513, 7475700942489200561,
		1108265084319566212, 1280462596732616140}
)

const (
	wz = uint8(64) // word size
)

type rollingHash interface {
	sum64() uint64
	write(c byte)
}

type cyclicPolynomial struct {
	front uint8
	size  uint8
	data  []byte
	hash  uint64
}

func barrelShifts(v uint64, n uint8) uint64 {
	return (v << n) | (v >> (wz - n))
}

func newRollingHash(window []byte) rollingHash {
	h := &cyclicPolynomial{
		front: 0,
		size:  uint8(len(window)),
		data:  make([]byte, uint8(len(window))),
		hash:  0}
	copy(h.data, window)
	for _, c := range h.data {
		h.hash = barrelShifts(h.hash, 1) ^ htbl[c]
	}
	return h
}

func (this *cyclicPolynomial) sum64() uint64 {
	return this.hash
}

func (this *cyclicPolynomial) write(c byte) {
	this.hash = barrelShifts(this.hash, 1) ^ barrelShifts(htbl[this.data[this.front]], this.size) ^ htbl[c]
	this.data[this.front] = c
	this.front = (this.front + 1) % this.size
}
