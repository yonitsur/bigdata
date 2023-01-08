package bigdatacourse.hw2.studentcode;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.*;
import java.io.File;  
import java.io.FileNotFoundException;  
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.CqlSession;
import bigdatacourse.hw2.HW2API;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

public class HW2StudentAnswer implements HW2API{
	
	// general consts
	public static final String		NOT_AVAILABLE_VALUE 	=		"na";
	public static final int		MAX_ROWS 	=		10000;
	// CQL stuff
	private static final String		TABLE_REVIEWS = "reviews_Office_Products";
	private static final String		TABLE_ITEMS = "meta_Office_Products";
	
	private static final String		CQL_CREATE_REVIEWS_TABLE = 
			"CREATE TABLE " + TABLE_REVIEWS 	+"(" 		+ 
				"reviewerID text,"			+
				"asin text,"				+
				"reviewerName text,"			+
				"helpful LIST<int>,"				+
				"reviewText text,"				+
				"overall float,"				+
				"summary text,"				+
				"unixReviewTime bigint,"			+
				"reviewTime text,"				+
				"PRIMARY KEY ((reviewerID), asin, unixReviewTime)"	+
			") "; //+"WITH CLUSTERING ORDER BY (unixReviewTime DESC, asin ASC, reviewerID ASC)";

	private static final String		CQL_CREATE_ITEMS_TABLE = 
			"CREATE TABLE " + TABLE_ITEMS 	+"(" 		+ 
				"asin text,"				+
				"title text,"				+
				"price float,"				+
				"imUrl text,"				+
				"related map<text, frozen <list<text>>>,"				+
				"salesRank MAP<text,bigint>,"				+
				"brand text,"				+
				"categories SET<text>,"				+
				"description text,"				+
				"PRIMARY KEY ((asin))"	+
			") ";
							
				
	private static final String		CQL_TABLE_REVIEWS_INSERT = 
			"INSERT INTO " + TABLE_REVIEWS + " (reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, unixReviewTime, reviewTime) " + 
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private static final String		CQL_TABLE_REVIEWS_SELECT = 
			"SELECT * FROM " + TABLE_REVIEWS + " WHERE reviewerID = ?";//" ORDER BY desc unixReviewTime, asin";

	private static final String		CQL_TABLE_REVIEWS_SELECT_BY_ITEM = 
			"SELECT * FROM " + TABLE_REVIEWS + " WHERE asin = ? ALLOW FILTERING";

	private static final String		CQL_TABLE_ITEMS_INSERT =
			"INSERT INTO " + TABLE_ITEMS + " (asin, title, price, imUrl, related, salesRank, brand, categories, description) " + 
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";	
	
	private static final String		CQL_TABLE_ITEMS_SELECT =
			"SELECT * FROM " + TABLE_ITEMS + " WHERE asin = ?";
	

	// cassandra session
	private CqlSession session;
	
	// prepared statements
	PreparedStatement reviewsAdd;
	PreparedStatement reviewsSelect;
	PreparedStatement reviewsSelectByItem;
	PreparedStatement itemAdd;
	PreparedStatement itemSelect;
	
	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}
		
		System.out.println("Initializing connection to Cassandra...");
		
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();
		
		System.out.println("Initializing connection to Cassandra... Done");
	}

	@Override
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}
		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	@Override
	public void createTables() {
		session.execute(CQL_CREATE_ITEMS_TABLE);
		System.out.println("created table: " + TABLE_ITEMS);
		session.execute(CQL_CREATE_REVIEWS_TABLE);
		System.out.println("created table: " + TABLE_REVIEWS);	
	}

	@Override
	public void initialize() {
		reviewsAdd = session.prepare(CQL_TABLE_REVIEWS_INSERT);
		reviewsSelect = session.prepare(CQL_TABLE_REVIEWS_SELECT);
		reviewsSelectByItem = session.prepare(CQL_TABLE_REVIEWS_SELECT_BY_ITEM);
		itemAdd = session.prepare(CQL_TABLE_ITEMS_INSERT);
		itemSelect = session.prepare(CQL_TABLE_ITEMS_SELECT);
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		pathItemsFile="C:\\Users\\yonit\\Studies\\bigdata\\ex2\\data\\meta_Office_Products.json";
		int maxThreads	= 250;	
		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		try {
			File meta_Office_Products = new File(pathItemsFile);
			Scanner mymetaReader = new Scanner(meta_Office_Products);
			int t=0;
			while (mymetaReader.hasNextLine() && t<MAX_ROWS) {	
				String asin				= NOT_AVAILABLE_VALUE;
				String title			= NOT_AVAILABLE_VALUE;
				String imUrl			= NOT_AVAILABLE_VALUE;
				String brand			= NOT_AVAILABLE_VALUE;
				String description		= NOT_AVAILABLE_VALUE;
				TreeSet<String> categories = null;
				Map<String, List<String>> related = null;
				Map<String, Long> salesRank = null;
				Float price = null;

				String data = mymetaReader.nextLine();
				JSONObject json		=	new JSONObject(data);

				try{asin 			=	json.getString("asin");}catch (JSONException e) {}
				try{imUrl			=	json.getString("imUrl");}catch (JSONException e) {}
				try{brand			=	json.getString("brand");}catch (JSONException e) {}
				try{description		=	json.getString("description");}catch (JSONException e) {}		
				try{title			=	json.getString("title");}catch (JSONException e) {}	
				try{price   =   (float) json.getDouble("price");}catch (JSONException e) {}
				try{related 		 = new HashMap<String, List<String>>();
					JSONObject json1 =	json.getJSONObject("related");
					JSONArray json2	 =	json1.getJSONArray("also_bought");
					JSONArray json3	 =	json1.getJSONArray("also_viewed");
					for (int i = 0; i < json2.length(); i++) {
						if (related.containsKey("also_bought")) 
							related.get("also_bought").add(json2.getString(i));
						else 
							related.put("also_bought", new ArrayList<String>(Arrays.asList(json2.getString(i))));
					}
					for (int i = 0; i < json3.length(); i++) {
						if (related.containsKey("also_viewed")) 
							related.get("also_viewed").add(json3.getString(i));
						else 
							related.put("also_viewed", new ArrayList<String>(Arrays.asList(json3.getString(i))));
					}
				}catch (JSONException e) {}
				try{salesRank = new HashMap<String, Long>();
					JSONObject json0	=	json.getJSONObject("salesRank");
					for (String key : json0.keySet()) 
						salesRank.put(key, json0.getLong(key));			
				}catch (JSONException e) {}
				try{categories = new TreeSet<String>();
					JSONArray json2		=	json.getJSONArray("categories");
					for (int i = 0; i < json2.length(); i++) {
						JSONArray json3 = json2.getJSONArray(i);
						for (int j = 0; j < json3.length(); j++) 
							categories.add(json3.getString(j));
					}
				}catch (JSONException e) {}
				
				BoundStatement bstmt = itemAdd.bind(asin, title, price, imUrl, related, salesRank, brand, categories, description);			
				executor.execute(new Runnable() {
					@Override
					public void run() {
						session.execute(bstmt);
					}
				});
				t++;
			}
			mymetaReader.close();
		}
		catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}

	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		pathReviewsFile = "C:\\Users\\yonit\\Studies\\bigdata\\ex2\\data\\reviews_Office_Products.json";
		int maxThreads	= 250;	
		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		try{
			File reviews_Office_Products = new File(pathReviewsFile);
			Scanner myreviewReader = new Scanner(reviews_Office_Products);
			int t=0;
			while (myreviewReader.hasNextLine() && t<MAX_ROWS) {
				String asin					= NOT_AVAILABLE_VALUE;
				String reviewerID			= NOT_AVAILABLE_VALUE;
				String reviewerName			= NOT_AVAILABLE_VALUE;
				String summary				= NOT_AVAILABLE_VALUE;
				String reviewTime			= NOT_AVAILABLE_VALUE;
				String reviewText			= NOT_AVAILABLE_VALUE;
				Float overall				= null;
				ArrayList<Integer> helpful	= null;
				Long unixReviewTime			= null;
				
				String data = myreviewReader.nextLine();
				JSONObject json		=	new JSONObject(data);	

				try{	asin	=	json.getString("asin");}catch (JSONException e) {}
				try{	reviewerID	=	json.getString("reviewerID");}catch (JSONException e) {}
				try{	reviewerName =	json.getString("reviewerName");}catch (JSONException e) {}
				try{	reviewText	=	json.getString("reviewText");}	catch (JSONException e) {}
				try{	overall	=	(float) json.getDouble("overall");}catch (JSONException e) {}
				try{	summary	=	json.getString("summary");}catch (JSONException e) {}
				try{	unixReviewTime	=	json.getLong("unixReviewTime");}catch (JSONException e) {}
				try{	reviewTime	=	json.getString("reviewTime");}catch (JSONException e) {}
				try{	helpful = new ArrayList<Integer>();
						JSONArray help	=	json.getJSONArray("helpful");
						for (int i = 0; i < help.length(); i++) 
							helpful.add(help.getInt(i));
				}	catch (JSONException e) {}
				BoundStatement bstmt = reviewsAdd.bind(reviewerID, asin, reviewerName, helpful, reviewText, overall, summary, unixReviewTime, reviewTime);
				executor.execute(new Runnable() {
					@Override
					public void run() {
						session.execute(bstmt);
					}
				});
				t++;
			}
			myreviewReader.close();
		}
		catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

	}

	@Override
	public void item(String asin) {
		
		BoundStatement bstmt = itemSelect.bind(asin);
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		if (row == null) {
			// required format - if the asin does not exists return this value
			System.out.println("not exists");
			return;
		}
		while (row != null) {
			try{
				System.out.println("asin: " 		+ row.getString("asin"));
			}
			catch (NullPointerException e) {
				System.out.println("asin: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("title: " 		+ row.getString("title"));
			}
			catch (NullPointerException e) {
				System.out.println("title: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("image: " 		+ row.getString("imUrl"));
			}
			catch (NullPointerException e) {
				System.out.println("image: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("categories: " 	+ row.getSet("categories", String.class));
			}
			catch (NullPointerException e) {
				System.out.println("categories: " 	+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("description: " 	+ row.getString("description"));
			}
			catch (NullPointerException e) {
				System.out.println("description: " 	+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("price: " 		+ row.getFloat("price"));
			}
			catch (NullPointerException e) {
				System.out.println("price: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("related: " 		+ row.getMap("related", String.class, ArrayList.class));
			}
			catch (NullPointerException e) {
				System.out.println("related: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("salesRank: " 	+ row.getMap("salesRank", String.class, Long.class));
			}
			catch (NullPointerException e) {
				System.out.println("salesRank: " 	+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("brand: " 		+ row.getString("brand"));
			}
			catch (NullPointerException e) {
				System.out.println("brand: " 		+ NOT_AVAILABLE_VALUE);
			}
			row = rs.one();
		}
		// required format - example for asin B005QB09TU
		// System.out.println("asin: " 		+ "B005QB09TU");
		// System.out.println("title: " 		+ "Circa Action Method Notebook");
		// System.out.println("image: " 		+ "http://ecx.images-amazon.com/images/I/41ZxT4Opx3L._SY300_.jpg");
		// System.out.println("categories: " 	+ new TreeSet<String>(Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper")));
		// System.out.println("description: " 	+ "Circa + Behance = Productivity. The minute-to-minute flexibility of Circa note-taking meets the organizational power of the Action Method by Behance. The result is enhanced productivity, so you'll formulate strategies and achieve objectives even more efficiently with this Circa notebook and project planner. Read Steve's blog on the Behance/Levenger partnership Customize with your logo. Corporate pricing available. Please call 800-357-9991.");;
		
		
	}
	
	
	@Override
	public void userReviews(String reviewerID) {
		// the order of the reviews should be by the time (desc), then by the asin
		// prints the user's reviews in a descending order (latest review is printed first)
		BoundStatement bstmt = reviewsSelect.bind(reviewerID);
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		if (row == null) {
			// required format - if the asin does not exists return this value
			System.out.println("not exists");
			return;
		}
		int count=0;
		while (row != null) {
			try{
				System.out.println("time: " 			+ Instant.ofEpochSecond(row.getLong("unixReviewTime")));
			}
			catch (NullPointerException e) {
				System.out.println("time: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("asin: " 			+ row.getString("asin"));
			}
			catch (NullPointerException e) {
				System.out.println("asin: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewerID: " 		+ row.getString("reviewerID"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewerID: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewerName: " 	+ row.getString("reviewerName"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewerName: " 	+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("rating: " 			+ row.getFloat("overall"));
			}
			catch (NullPointerException e) {
				System.out.println("rating: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("summary: " 			+ row.getString("summary"));
			}
			catch (NullPointerException e) {
				System.out.println("summary: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewText: " 		+ row.getString("reviewText"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewText: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("helpful: " 			+ row.getList("helpful", Integer.class));
			}
			catch (NullPointerException e) {
				System.out.println("helpful: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewTime: " 		+ row.getString("reviewTime"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewTime: " 		+ NOT_AVAILABLE_VALUE);
			}
			row = rs.one();
			count++;
		}
		
		
		// required format - example for reviewerID A17OJCRPMYWXWV
		// System.out.println(	
		// 		"time: " 			+ Instant.ofEpochSecond(1362614400) + 
		// 		", asin: " 			+ "B005QDG2AI" 	+
		// 		", reviewerID: " 	+ "A17OJCRPMYWXWV" 	+
		// 		", reviewerName: " 	+ "Old Flour Child"	+
		// 		", rating: " 		+ 5 	+ 
		// 		", summary: " 		+ "excellent quality"	+
		// 		", reviewText: " 	+ "These cartridges are excellent .  I purchased them for the office where I work and they perform  like a dream.  They are a fraction of the price of the brand name cartridges.  I will order them again!");

		// System.out.println(	
		// 		"time: " 			+ Instant.ofEpochSecond(1360108800) + 
		// 		", asin: " 			+ "B003I89O6W" 	+
		// 		", reviewerID: " 	+ "A17OJCRPMYWXWV" 	+
		// 		", reviewerName: " 	+ "Old Flour Child"	+
		// 		", rating: " 		+ 5 	+ 
		// 		", summary: " 		+ "Checkbook Cover"	+
		// 		", reviewText: " 	+ "Purchased this for the owner of a small automotive repair business I work for.  The old one was being held together with duct tape.  When I saw this one on Amazon (where I look for almost everything first) and looked at the price, I knew this was the one.  Really nice and very sturdy.");

		System.out.println("total reviews: " + count);
	}

	@Override
	public void itemReviews(String asin) {
		// the order of the reviews should be by the time (desc), then by the reviewerID
		// prints the items's reviews in a descending order (latest review is printed first)
		BoundStatement bstmt = reviewsSelectByItem.bind(asin);
		ResultSet rs = session.execute(bstmt);
		Row row = rs.one();
		if (row == null) {
			System.out.println("not exists");
			return;
		}
		int count=0;
		while (row != null) {
			try{
				System.out.println("time: " 			+ Instant.ofEpochSecond(row.getLong("unixReviewTime")));
			}
			catch (NullPointerException e) {
				System.out.println("time: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("asin: " 			+ row.getString("asin"));
			}
			catch (NullPointerException e) {
				System.out.println("asin: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewerID: " 		+ row.getString("reviewerID"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewerID: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewerName: " 	+ row.getString("reviewerName"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewerName: " 	+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("rating: " 			+ row.getFloat("overall"));
			}
			catch (NullPointerException e) {
				System.out.println("rating: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("summary: " 			+ row.getString("summary"));
			}
			catch (NullPointerException e) {
				System.out.println("summary: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewText: " 		+ row.getString("reviewText"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewText: " 		+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("helpful: " 			+ row.getList("helpful", Integer.class));
			}
			catch (NullPointerException e) {
				System.out.println("helpful: " 			+ NOT_AVAILABLE_VALUE);
			}
			try{
				System.out.println("reviewTime: " 		+ row.getString("reviewTime"));
			}
			catch (NullPointerException e) {
				System.out.println("reviewTime: " 		+ NOT_AVAILABLE_VALUE);
			}
			row = rs.one();
			count++;
		}
		
		



		// required format - example for asin B005QDQXGQ
		// System.out.println(	
		// 		"time: " 			+ Instant.ofEpochSecond(1391299200) + 
		// 		", asin: " 			+ "B005QDQXGQ" 	+
		// 		", reviewerID: " 	+ "A1I5J5RUJ5JB4B" 	+
		// 		", reviewerName: " 	+ "T. Taylor \"jediwife3\""	+
		// 		", rating: " 		+ 5 	+ 
		// 		", summary: " 		+ "Play and Learn"	+
		// 		", reviewText: " 	+ "The kids had a great time doing hot potato and then having to answer a question if they got stuck with the &#34;potato&#34;. The younger kids all just sat around turnin it to read it.");

		// System.out.println(	
		// 		"time: " 			+ Instant.ofEpochSecond(1390694400) + 
		// 		", asin: " 			+ "B005QDQXGQ" 	+
		// 		", reviewerID: " 	+ "AF2CSZ8IP8IPU" 	+
		// 		", reviewerName: " 	+ "Corey Valentine \"sue\""	+
		// 		", rating: " 		+ 1 	+ 
		// 		", summary: " 		+ "Not good"	+
		// 		", reviewText: " 	+ "This Was not worth 8 dollars would not recommend to others to buy for kids at that price do not buy");

		// System.out.println(	
		// 		"time: "			+ Instant.ofEpochSecond(1388275200) + 
		// 		", asin: " 			+ "B005QDQXGQ" 	+
		// 		", reviewerID: " 	+ "A27W10NHSXI625" 	+
		// 		", reviewerName: " 	+ "Beth"	+
		// 		", rating: " 		+ 2 	+ 
		// 		", summary: " 		+ "Way overpriced for a beach ball"	+
		// 		", reviewText: " 	+ "It was my own fault, I guess, for not thoroughly reading the description, but this is just a blow-up beach ball.  For that, I think it was very overpriced.  I thought at least I was getting one of those pre-inflated kickball-type balls that you find in the giant bins in the chain stores.  This did have a page of instructions for a few different games kids can play.  Still, I think kids know what to do when handed a ball, and there's a lot less you can do with a beach ball than a regular kickball, anyway.");

		System.out.println("total reviews: " + count);
	}


}
