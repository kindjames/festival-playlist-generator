using System;
using System.IO;
using System.Net;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Runtime.Serialization;
using MongoDB.Driver;

namespace ConsoleApp
{
	class MainClass
	{
		[DataContract]
		public class ResultSetMetaData
		{
			[DataMember(Name = "page")]
			public int Page { get; set; }

			[DataMember(Name = "per_page")]
			public int PerPage { get; set; }

			[DataMember(Name = "total")]
			public int Total { get; set; }
		}

		[DataContract]
		public class EventResultSet
		{
			[DataMember(Name = "meta")]
			public ResultSetMetaData Meta { get; set; }

			[DataMember(Name = "events")]
			public ICollection<Event> Events { get; set; }
		}

		public class DatabaseResultSet
		{
			public DateTime Time { get; set; }

			public string Country { get; set; }

			public ICollection<Event> Events { get; set; }
		}

		[DataContract]
		public class Event
		{
			[DataMember(Name = "id")]
			public int Id { get; set; }

			[DataMember(Name = "title")]
			public string Title { get; set; }

			[DataMember(Name = "short_title")]
			public string ShortTitle { get; set; }

			[DataMember(Name = "datetime_local")]
			public DateTime Date { get; set; }

			[DataMember(Name = "score")]
			public float? Score { get; set; }

			[DataMember(Name = "performers")]
			public ICollection<Performer> Performers { get; set; }
		}

		[DataContract]
		public class Performer
		{
			[DataMember(Name = "id")]
			public int Id { get; set; }

			[DataMember(Name = "name")]
			public string Name { get; set; }

			[DataMember(Name = "short_name")]
			public string ShortName { get; set; }

			[DataMember(Name = "type")]
			public string Type { get; set; }

			[DataMember(Name = "score")]
			public float? Score { get; set; }
		}

		static void PrintEvents(IEnumerable<Event> events)
		{
			foreach (var festival in events)
			{
				Console.WriteLine(festival.Title);

				foreach (var performer in festival.Performers)
				{
					Console.WriteLine("\t" + performer.Name + " (Id: " + performer.Id + ")");
				}

				Console.WriteLine();
			}
		}

		static IEnumerable<Event> GetAllEventsFromApi(string country)
		{
			var page = 1;
			const int perPage = 100;

			Console.WriteLine("Getting events from API...");

			var initialResultSet = GetEvents(page, perPage, country);

			Console.WriteLine(initialResultSet.Meta.Total + " events found...");

			var events = new List<Event>(initialResultSet.Meta.Total);

			if (initialResultSet.Events.Count > 0)
			{
				events.AddRange(initialResultSet.Events);

				while (events.Count < initialResultSet.Meta.Total)
				{
					page++;
					var resultSet = GetEvents(page, perPage, country);

					events.AddRange(resultSet.Events);
				}
			}

			return events;
		}

		static EventResultSet GetEvents(int page, int perPage, string country)
		{
			Console.WriteLine("Getting page " + page + "...");

			var url = "http://api.seatgeek.com/2/events?taxonomies.id=2010000&sort=score.desc&per_page=" + perPage + "&page=" + page;

			if (!String.IsNullOrEmpty(country))
			{
				url += "&venue.country=" + country;
			}

			var webRequest = (HttpWebRequest) WebRequest.Create(url);

			using (var webResponse = webRequest.GetResponse())
			using (var stream = new StreamReader(webResponse.GetResponseStream()))
			{
				var json = stream.ReadToEnd();

				return JsonConvert.DeserializeObject<EventResultSet>(json);
			}
		}

		public static void Main(string[] args)
		{
			var connectionString = "mongodb://localhost";
			var client = new MongoClient(connectionString);
			var server = client.GetServer();

			var seatgeekDb = server.GetDatabase("seatgeek");

			var resultsetCollection = seatgeekDb.GetCollection<DatabaseResultSet>("resultsets");

			Console.WriteLine("Type country, e.g. us, uk, etc (or leave blank)...");

			var country = Console.ReadLine();

			var eventsFromApi = GetAllEventsFromApi(country).ToList();

			resultsetCollection.Insert<DatabaseResultSet>(new DatabaseResultSet {
				Time = DateTime.Now,
				Events = eventsFromApi,
				Country = "us",
			});

			Console.WriteLine("Dumped " + eventsFromApi.Count() + " event records to the database.");
		}
	}
}