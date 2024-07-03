package prov.idea.evaluation;

import java.util.Random;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;


//Main code obtained from https://github.com/quantumdb

//@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class UserUtils {

	/**
	 * Java class obtained from https://github.com/quantumdb/releng-demo/blob/master/src/main/java/io/quantumdb/releng/demo/utils/UserUtils.java
	 * Popular male names in Florence, Italy (1427): http://www.behindthename.com/top/lists/ih/1427
	 */
	private static final String[] NAMES = {
			"Abramo", "Adamo", "Adovardo", "Agnolino", "Agnolo", "Agostino", "Alamanno", "Alberto", "Albizzo",
			"Alessandro", "Alesso", "Ambrogio", "Amerigo", "Amideo", "Andrea", "Anichino", "Antonio", "Apollonio",
			"Arrigo", "Attaviano", "Averardo", "Baldassarr", "Baldo", "Banco", "Bandino", "Bardo", "Barone", "Bartolo",
			"Bartolomeo", "Bastiano", "Battista", "Bencivenni", "Benedetto", "Benino", "Benozzo", "Benvenuto",
			"Bernaba", "Bernardo", "Bertino", "Berto", "Bettino", "Betto", "Biagio", "Bianco", "Bindo", "Boccaccio",
			"Bonacorso", "Bonaguida", "Bonaiuto", "Bonifazio", "Bonino", "Bono", "Bonsi", "Bruno", "Buccio", "Buono",
			"Buto", "Cambino", "Cambio", "Cardinale", "Carlo", "Castello", "Cecco", "Cenni", "Chiaro", "Chimenti",
			"Chimento", "Cino", "Cione", "Cipriano", "Cola", "Conte", "Corrado", "Corso", "Cosimo", "Cristofano",
			"Daddo", "Daniello", "Dego", "Deo", "Diedi", "Dino", "Doffo", "Domenico", "Donato", "Donnino", "Duccio",
			"Fabiano", "Fede", "Federigo", "Felice", "Feo", "Filippo", "Francesco", "Franco", "Frosino", "Gabbriello",
			"Gentile", "Geri", "Gherardino", "Gherardo", "Ghirigoro", "Giannino", "Giannozzo", "Giano", "Gino",
			"Giorgio", "Giovacchin", "Giovanni", "Giovannozz", "Giovenco", "Girolamo", "Giunta", "Giusto", "Goro",
			"Grazia", "Gualterott", "Gualtieri", "Guasparre", "Guccio", "Guelfo", "Guglielmo", "Guido", "Iacomo",
			"Iacopo", "Lamberto", "Lando", "Lapaccio", "Lapo", "Lazzero", "Leonardo", "Lippo", "Lodovico", "Lorenzo",
			"Lotto", "Luca", "Luigi", "Maccio", "Maffeo", "Mainardo", "Manetto", "Manno", "Marchionne", "Marco",
			"Mariano", "Marino", "Mariotto", "Martino", "Maso", "Matteo", "Meo", "Michele", "Migliore", "Miniato",
			"Mino", "Monte", "Naldo", "Nanni", "Nannino", "Nardo", "Nello", "Nencio", "Neri", "Niccola", "Niccolaio",
			"Niccolino", "Niccolo", "Nigi", "Nofri", "Nozzo", "Nuccio", "Nuto", "Orlando", "Ormanno", "Pace", "Pacino",
			"Pagolo", "Palla", "Pandolfo", "Papi", "Pasquino", "Piero", "Pierozzo", "Pietro", "Pippo", "Polito",
			"Priore", "Puccino", "Puccio", "Ramondo", "Riccardo", "Ricco", "Ridolfo", "Rinaldo", "Rinieri", "Ristoro",
			"Roberto", "Romigi", "Romolo", "Rosso", "Ruggieri", "Salvadore", "Salvestro", "Salvi", "Sandro", "Santi",
			"Scolaio", "Simone", "Sinibaldo", "Smeraldo", "Spinello", "Stagio", "Stefano", "Stoldo", "Strozza",
			"Taddeo", "Tano", "Tieri", "Tingo", "Tommaso", "Tomme", "Ubertino", "Uberto", "Ugo", "Ugolino", "Uguccione",
			"Urbano", "Vanni", "Vannozzo", "Ventura", "Vettorio", "Vico", "Vieri", "Vincenzo", "Zaccheria", "Zanobi"
	};


	
	private static final String[] SURNAMES = {
			"AbramoSN", "AdamoSN", "AdovardoSN", "AgnolinoSN", "AgnoloSN", "AgostinoSN", "AlamannoSN", "AlbertoSN", "AlbizzoSN",
			"AlessandroSN", "AlessoSN", "AmbrogioSN", "AmerigoSN", "AmideoSN", "AndreaSN", "AnichinoSN", "AntonioSN", "ApollonioSN",
			"ArrigoSN", "AttavianoSN", "AverardoSN", "BaldassarrSN", "BaldoSN", "BancoSN", "BandinoSN", "BardoSN", "BaroneSN", "BartoloSN",
			"BartolomeoSN", "BastianoSN", "BattistaSN", "BencivenniSN", "BenedettoSN", "BeninoSN", "BenozzoSN", "BenvenutoSN",
			"BernabaSN", "BernardoSN", "BertinoSN", "BertoSN", "BettinoSN", "BettoSN", "BiagioSN", "BiancoSN", "BindoSN", "BoccaccioSN",
			"BonacorsoSN", "BonaguidaSN", "BonaiutoSN", "BonifazioSN", "BoninoSN", "BonoSN", "BonsiSN", "BrunoSN", "BuccioSN", "BuonoSN",
			"ButoSN", "CambinoSN", "CambioSN", "CardinaleSN", "CarloSN", "CastelloSN", "CeccoSN", "CenniSN", "ChiaroSN", "ChimentiSN",
			"ChimentoSN", "CinoSN", "CioneSN", "CiprianoSN", "ColaSN", "ConteSN", "CorradoSN", "CorsoSN", "CosimoSN", "CristofanoSN",
			"DaddoSN", "DanielloSN", "DegoSN", "DeoSN", "DiediSN", "DinoSN", "DoffoSN", "DomenicoSN", "DonatoSN", "DonninoSN", "DuccioSN",
			"FabianoSN", "FedeSN", "FederigoSN", "FeliceSN", "FeoSN", "FilippoSN", "FrancescoSN", "FrancoSN", "FrosinoSN", "GabbrielloSN",
			"GentileSN", "GeriSN", "GherardinoSN", "GherardoSN", "GhirigoroSN", "GianninoSN", "GiannozzoSN", "GianoSN", "GinoSN",
			"GiorgioSN", "GiovacchinSN", "GiovanniSN", "GiovannozzSN", "GiovencoSN", "GirolamoSN", "GiuntaSN", "GiustoSN", "GoroSN",
			"GraziaSN", "GualterottSN", "GualtieriSN", "GuasparreSN", "GuccioSN", "GuelfoSN", "GuglielmoSN", "GuidoSN", "IacomoSN",
			"IacopoSN", "LambertoSN", "LandoSN", "LapaccioSN", "LapoSN", "LazzeroSN", "LeonardoSN", "LippoSN", "LodovicoSN", "LorenzoSN",
			"LottoSN", "LucaSN", "LuigiSN", "MaccioSN", "MaffeoSN", "MainardoSN", "ManettoSN", "MannoSN", "MarchionneSN", "MarcoSN",
			"MarianoSN", "MarinoSN", "MariottoSN", "MartinoSN", "MasoSN", "MatteoSN", "MeoSN", "MicheleSN", "MiglioreSN", "MiniatoSN",
			"MinoSN", "MonteSN", "NaldoSN", "NanniSN", "NanninoSN", "NardoSN", "NelloSN", "NencioSN", "NeriSN", "NiccolaSN", "NiccolaioSN",
			"NiccolinoSN", "NiccoloSN", "NigiSN", "NofriSN", "NozzoSN", "NuccioSN", "NutoSN", "OrlandoSN", "OrmannoSN", "PaceSN", "PacinoSN",
			"PagoloSN", "PallaSN", "PandolfoSN", "PapiSN", "PasquinoSN", "PieroSN", "PierozzoSN", "PietroSN", "PippoSN", "PolitoSN",
			"PrioreSN", "PuccinoSN", "PuccioSN", "RamondoSN", "RiccardoSN", "RiccoSN", "RidolfoSN", "RinaldoSN", "RinieriSN", "RistoroSN",
			"RobertoSN", "RomigiSN", "RomoloSN", "RossoSN", "RuggieriSN", "SalvadoreSN", "SalvestroSN", "SalviSN", "SandroSN", "SantiSN",
			"ScolaioSN", "SimoneSN", "SinibaldoSN", "SmeraldoSN", "SpinelloSN", "StagioSN", "StefanoSN", "StoldoSN", "StrozzaSN",
			"TaddeoSN", "TanoSN", "TieriSN", "TingoSN", "TommasoSN", "TommeSN", "UbertinoSN", "UbertoSN", "UgoSN", "UgolinoSN", "UguccioneSN",
			"UrbanoSN", "VanniSN", "VannozzoSN", "VenturaSN", "VettorioSN", "VicoSN", "VieriSN", "VincenzoSN", "ZaccheriaSN", "ZanobiSN"
	};

	public static String pickName() {
		Random random = new Random();
		return pickName(random);
	}

	public static String getEmail(String name) {
		return name.toLowerCase() + "@florence.it";
	}

	public static String pickName(Random random) {
		int index = random.nextInt(NAMES.length);
		return NAMES[index];
	}
	
	public static String pickSurname(Random random) {
		int index = random.nextInt(SURNAMES.length);
		return SURNAMES[index];
	}



}