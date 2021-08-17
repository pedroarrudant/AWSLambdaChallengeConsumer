using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Newtonsoft.Json;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AWSLambdaChallengeConsumer
{
    public class Function
    {
        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {

        }


        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
        {
            foreach (var message in evnt.Records)
            {
                await ProcessMessageAsync(message, context);
            }
        }

        private async Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
        {
            context.Logger.LogLine($"Processed message {message.Body}");

            var dynamoDB = new AmazonDynamoDBClient();
            var DBName = "ChallengeFIAP";
            Table table = Table.LoadTable(dynamoDB, DBName);

            if (SanitizarRegistro(JsonConvert.DeserializeObject<Registro>(message.Body)))
            {
                var document = Document.FromJson(message.Body);
                await table.PutItemAsync(document);
            }
        }

        private bool SanitizarRegistro(Registro Registro)
        {
            bool insert = true;

            if (Registro.Indice == null && Registro.Modelo == null && Registro.Positivo == null && Registro.Restritivo == null && Registro.Score == null)
            {
                insert = false;
            }

            if (Registro.Indice == "" && Registro.Modelo == "" && Registro.Positivo == "" && Registro.Restritivo == "" && Registro.Score == "")
            {
                insert = false;
            }

            if (!DateTime.TryParseExact(Registro.AnoMesDia, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
            {
                insert = false;
            }

            return insert;
        }
    }
}
