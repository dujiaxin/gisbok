from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime, time
import json


def invertedIndex2string(jsonstring):
    jsonObjects = json.loads(jsonstring[2], strict=False)
    string_list = ["[PAD]"] * jsonObjects["IndexLength"]
    for k,v in jsonObjects['InvertedIndex'].items():
        for vv in v:
            string_list[vv] = k
    jsonstring[2] = " ".join(string_list)
    return tuple(jsonstring[0],jsonstring[1],jsonstring[2])

class MicrosoftAcademicGraph():
    datatypedict = {
        'int': IntegerType(),
        'uint': IntegerType(),
        'long': LongType(),
        'ulong': LongType(),
        'float': FloatType(),
        'string': StringType(),
        'DateTime': DateType(),
    }

    def __init__(self):
        self.sc = SparkContext("local[*]", "First App")
        self.sqlContext = SQLContext(self.sc)

    def getFullpath(self, streamName):
        return 'file:///xye_data_nobackup/mag/' + self.streams[streamName][0]

    # return stream schema
    def getSchema(self, streamName):
        schema = StructType()
        for field in self.streams[streamName][1]:
            fieldname, fieldtype = field.split(':')
            nullable = fieldtype.endswith('?')
            if nullable:
                fieldtype = fieldtype[:-1]
            schema.add(StructField(fieldname, self.datatypedict[fieldtype], nullable))
        return schema

    # return stream dataframe
    def getDataframe(self, streamName):
        return self.sqlContext.read.format('com.databricks.spark.csv').options(header='false', delimiter='\t').schema(
            self.getSchema(streamName)).load(self.getFullpath(streamName))

    def save(self, df, streamName, coalesce=True):
        _path = self.getFullpath(streamName)
        print('saving to ' + _path)
        if coalesce:
            df.coalesce(1).write.mode('overwrite').format('com.databricks.spark.csv')\
                .options(header='false', delimiter='\t') \
                .save(_path)
        else:
            df.write.mode('overwrite').format('com.databricks.spark.csv')\
                .options(header='true', delimiter='\t') \
                .option("escape", "") \
                .option("quoteMode", "NONE") \
                .save(_path)

    # define stream dictionary
    streams = {
        'Affiliations': ('mag/Affiliations.txt',
                         ['AffiliationId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string',
                          'GridId:string', 'OfficialPage:string', 'WikiPage:string', 'PaperCount:long',
                          'CitationCount:long', 'Latitude:float?', 'Longitude:float?', 'CreatedDate:DateTime']),
        'Authors': ('mag/Authors.txt', ['AuthorId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string',
                                        'LastKnownAffiliationId:long?', 'PaperCount:long', 'CitationCount:long',
                                        'CreatedDate:DateTime']),
        'ConferenceInstances': ('mag/ConferenceInstances.txt',
                                ['ConferenceInstanceId:long', 'NormalizedName:string', 'DisplayName:string',
                                 'ConferenceSeriesId:long', 'Location:string', 'OfficialUrl:string',
                                 'StartDate:DateTime?', 'EndDate:DateTime?', 'AbstractRegistrationDate:DateTime?',
                                 'SubmissionDeadlineDate:DateTime?', 'NotificationDueDate:DateTime?',
                                 'FinalVersionDueDate:DateTime?', 'PaperCount:long', 'CitationCount:long',
                                 'Latitude:float?', 'Longitude:float?', 'CreatedDate:DateTime']),
        'ConferenceSeries': ('mag/ConferenceSeries.txt',
                             ['ConferenceSeriesId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string',
                              'PaperCount:long', 'CitationCount:long', 'CreatedDate:DateTime']),
        'EntityRelatedEntities': ('advanced/EntityRelatedEntities.txt',
                                  ['EntityId:long', 'EntityType:string', 'RelatedEntityId:long',
                                   'RelatedEntityType:string', 'RelatedType:int', 'Score:float']),
        'FieldOfStudyChildren': (
        'advanced/FieldOfStudyChildren.txt', ['FieldOfStudyId:long', 'ChildFieldOfStudyId:long']),
        'FieldOfStudyExtendedAttributes': ('advanced/FieldOfStudyExtendedAttributes.txt',
                                           ['FieldOfStudyId:long', 'AttributeType:int', 'AttributeValue:string']),
        'FieldsOfStudy': ('advanced/FieldsOfStudy.txt',
                          ['FieldOfStudyId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string',
                           'MainType:string', 'Level:int', 'PaperCount:long', 'CitationCount:long',
                           'CreatedDate:DateTime']),
        'GISFieldsOfStudy': ('gis/gisfields3.txt',
                          ['FieldOfStudyId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string',
                           'MainType:string', 'Level:int', 'PaperCount:long', 'CitationCount:long',
                           'CreatedDate:DateTime']),
        'Journals': ('mag/Journals.txt',
                     ['JournalId:long', 'Rank:uint', 'NormalizedName:string', 'DisplayName:string', 'Issn:string',
                      'Publisher:string', 'Webpage:string', 'PaperCount:long', 'CitationCount:long',
                      'CreatedDate:DateTime']),
        'PaperAbstractsInvertedIndex': (
        'nlp/PaperAbstractsInvertedIndex.txt.{*}', ['PaperId:long', 'IndexedAbstract:string']),
        'GisPaperAbstractsInvertedIndex': (
            'gis/GisPaperAbstractsInvertedIndex.txt', ['PaperId:long', 'IndexedAbstract:string']),
        'PaperAuthorAffiliations': ('mag/PaperAuthorAffiliations.txt',
                                    ['PaperId:long', 'AuthorId:long', 'AffiliationId:long?',
                                     'AuthorSequenceNumber:uint', 'OriginalAuthor:string',
                                     'OriginalAffiliation:string']),
        'PaperCitationContexts': (
        'nlp/PaperCitationContexts.txt', ['PaperId:long', 'PaperReferenceId:long', 'CitationContext:string']),
        'PaperExtendedAttributes': (
        'mag/PaperExtendedAttributes.txt', ['PaperId:long', 'AttributeType:int', 'AttributeValue:string']),
        'PaperFieldsOfStudy': (
        'advanced/PaperFieldsOfStudy.txt', ['PaperId:long', 'FieldOfStudyId:long', 'Score:float']),
        'PaperRecommendations': (
        'advanced/PaperRecommendations.txt', ['PaperId:long', 'RecommendedPaperId:long', 'Score:float']),
        'PaperReferences': ('mag/PaperReferences.txt', ['PaperId:long', 'PaperReferenceId:long']),
        'PaperResources': ('mag/PaperResources.txt',
                           ['PaperId:long', 'ResourceType:int', 'ResourceUrl:string', 'SourceUrl:string',
                            'RelationshipType:int']),
        'PaperUrls': (
        'mag/PaperUrls.txt', ['PaperId:long', 'SourceType:int?', 'SourceUrl:string', 'LanguageCode:string']),
        'Papers': ('mag/Papers.txt', ['PaperId:long', 'Rank:uint', 'Doi:string', 'DocType:string', 'PaperTitle:string',
                                      'OriginalTitle:string', 'BookTitle:string', 'Year:int?', 'Date:DateTime?',
                                      'Publisher:string', 'JournalId:long?', 'ConferenceSeriesId:long?',
                                      'ConferenceInstanceId:long?', 'Volume:string', 'Issue:string', 'FirstPage:string',
                                      'LastPage:string', 'ReferenceCount:long', 'CitationCount:long',
                                      'EstimatedCitation:long', 'OriginalVenue:string', 'FamilyId:long?',
                                      'CreatedDate:DateTime']),
        'GisPapers': ('gis/GisPapers.txt', ['PaperId:long', 'Rank:uint', 'Doi:string', 'DocType:string', 'PaperTitle:string',
                                      'OriginalTitle:string', 'BookTitle:string', 'Year:int?', 'Date:DateTime?',
                                      'Publisher:string', 'JournalId:long?', 'ConferenceSeriesId:long?',
                                      'ConferenceInstanceId:long?', 'Volume:string', 'Issue:string', 'FirstPage:string',
                                      'LastPage:string', 'ReferenceCount:long', 'CitationCount:long',
                                      'EstimatedCitation:long', 'OriginalVenue:string', 'FamilyId:long?',
                                      'CreatedDate:DateTime']),
        'GisPapersEn': (
        'gis/GisPapersEn.txt', ['PaperId:long', 'Rank:uint', 'Doi:string', 'DocType:string', 'PaperTitle:string',
                              'OriginalTitle:string', 'BookTitle:string', 'Year:int?', 'Date:DateTime?',
                              'Publisher:string', 'JournalId:long?', 'ConferenceSeriesId:long?',
                              'ConferenceInstanceId:long?', 'Volume:string', 'Issue:string', 'FirstPage:string',
                              'LastPage:string', 'ReferenceCount:long', 'CitationCount:long',
                              'EstimatedCitation:long', 'OriginalVenue:string', 'FamilyId:long?',
                              'CreatedDate:DateTime']),
        'GisTitleAbstracts': (
            'gis/GisTitleAbstracts.txt', ['PaperId:long', 'PaperTitle:string','IndexedAbstract:string']),
        'RelatedFieldOfStudy': ('advanced/RelatedFieldOfStudy.txt',
                                ['FieldOfStudyId1:long', 'Type1:string', 'FieldOfStudyId2:long', 'Type2:string',
                                 'Rank:float']),
    }


if __name__ == '__main__':
    MAG = MicrosoftAcademicGraph()

    # GisFileds = MAG.getDataframe('GISFieldsOfStudy')
    #
    # PaperFieldsOfStudy = MAG.getDataframe('PaperFieldsOfStudy')
    #
    # pf = PaperFieldsOfStudy.join(GisFileds, PaperFieldsOfStudy.FieldOfStudyId == GisFileds.FieldOfStudyId, 'left_semi')\
    #   .select(PaperFieldsOfStudy.PaperId)\
    #   .distinct()\
    #   .alias('pf')
    #
    # print("unique paper id:")
    # print(pf.count())
    #
    # Papers = MAG.getDataframe('Papers')
    # p = Papers.join(pf, Papers.PaperId == pf.PaperId, 'left_semi')        \
    #     .alias('p')
    # print("GIS paper all:")
    # print(p.count())
    # GisPapers = p.where(p.EstimatedCitation > 0)
    #     # .select(F.when(p.FamilyId.isNull(), p.PaperId).otherwise(p.FamilyId).alias('PaperId'), \
    #     #         p.EstimatedCitation)
    # MAG.save(GisPapers, 'GisPapers')
    GisPapers = MAG.getDataframe('GisPapersEn')
    # print("GisPapers.count()")
    # print(GisPapers.count())
    PaperUrls = MAG.getDataframe('PaperUrls')
    GisPapers.join(PaperUrls.where(PaperUrls.LanguageCode=='en'), GisPapers.PaperId == PaperUrls.PaperId, 'inner')\
        .select(PaperUrls.SourceUrl).write.format('csv').option("header", "true")\
        .save("file:///xye_data_nobackup/mag/gisUrl.csv")
    # print(gp.count())
    # gp.show(3)
    # MAG.save(gp, 'GisPapersEn')
    # print("Date:")
    # print(GisPapers.select(F.min("Date"), F.max("Date")).first())
    # print("Year:")
    # print(GisPapers.select(F.min("Year"), F.max("Year")).first())
    # print("CreatedDate:")
    # print(GisPapers.select(F.min("CreatedDate"), F.max("CreatedDate")).first())
    # df = MAG.sqlContext.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
    #
    #
    # newGisPapers = GisPapers.where(GisPapers.Date>df.select(F.to_date(df.t).alias('date')).first())
    # print('newGisPapers.count()')
    # print(newGisPapers.count())
    # PaperAbstractsInvertedIndex = MAG.getDataframe('PaperAbstractsInvertedIndex')
    # GisPaperAbstractsInvertedIndex = GisPapers.join(PaperAbstractsInvertedIndex, GisPapers.PaperId == PaperAbstractsInvertedIndex.PaperId, 'inner')\
    #                                             .select(GisPapers.PaperId, GisPapers.PaperTitle, PaperAbstractsInvertedIndex.IndexedAbstract)\
    #                                             .distinct()
    # print("GisPaperAbstractsInvertedIndex.count():")
    # print(GisPaperAbstractsInvertedIndex.count())
    # GisPaperAbstractsInvertedIndex = MAG.getDataframe('GisPaperAbstractsInvertedIndex')
    # GisTitleAbstracts = MAG.getDataframe('GisTitleAbstracts')

    # MAG.save(GisPaperAbstractsInvertedIndex,'GisTitleAbstracts')
    # GisTA = GisPaperAbstractsInvertedIndex.foreach(invertedIndex2string)
    # GisTA.show(3)
    # # Get affiliations
    # Affiliations = MAG.getDataframe('Affiliations')
    # Affiliations = Affiliations.select(Affiliations.AffiliationId, Affiliations.DisplayName)
    # # Affiliations.show(3)
    # # Get authors
    # Authors = MAG.getDataframe('Authors')
    # Authors = Authors.select(Authors.AuthorId, Authors.DisplayName, Authors.LastKnownAffiliationId, Authors.PaperCount)
    # # Authors.show(3)
    # # Get (author, paper) pairs
    # PaperAuthorAffiliations = MAG.getDataframe('PaperAuthorAffiliations')
    # AuthorPaper = PaperAuthorAffiliations.select(PaperAuthorAffiliations.AuthorId,
    #                                              PaperAuthorAffiliations.PaperId).distinct()
    # # AuthorPaper.show(3)
    # # # Get (Paper, EstimatedCitation).
    # # # Treat papers with same FamilyId as a single paper and sum the EstimatedCitation
    # # Papers = MAG.getDataframe('Papers')
    # p = GisPapers.where(GisPapers.EstimatedCitation > 0) \
    #     .select(F.when(GisPapers.FamilyId.isNull(), GisPapers.PaperId).otherwise(GisPapers.FamilyId).alias('PaperId'), \
    #             GisPapers.EstimatedCitation) \
    #     .alias('p')
    #
    # PaperCitation = p \
    #     .groupBy(p.PaperId) \
    #     .agg(F.sum(p.EstimatedCitation).alias('EstimatedCitation'))
    # #
    # # Generate author, paper, citation table
    # AuthorPaperCitation = AuthorPaper \
    #     .join(PaperCitation, AuthorPaper.PaperId == PaperCitation.PaperId, 'inner') \
    #     .select(AuthorPaper.AuthorId, AuthorPaper.PaperId, PaperCitation.EstimatedCitation)
    # #
    # # Order author, paper by citation
    # AuthorPaperOrderByCitation = AuthorPaperCitation \
    #     .withColumn('Rank', F.row_number().over(Window.partitionBy('AuthorId').orderBy(F.desc('EstimatedCitation'))))
    # #
    # # Generate author hindex
    # ap = AuthorPaperOrderByCitation.alias('ap')
    # AuthorHIndexTemp = ap \
    #     .groupBy(ap.AuthorId) \
    #     .agg(F.sum(ap.EstimatedCitation).alias('TotalEstimatedCitation'), \
    #          F.max(F.when(ap.EstimatedCitation >= ap.Rank, ap.Rank).otherwise(0)).alias('HIndex'))
    #
    # # Get author detail information
    # i = AuthorHIndexTemp.alias('i')
    # a = Authors.alias('a')
    # af = Affiliations.alias('af')
    #
    # AuthorHIndex = i \
    #     .join(a, a.AuthorId == i.AuthorId, 'inner') \
    #     .join(af, a.LastKnownAffiliationId == af.AffiliationId, 'outer') \
    #     .select(i.AuthorId, a.DisplayName, af.DisplayName.alias('AffiliationDisplayName'), a.PaperCount,
    #             i.TotalEstimatedCitation, i.HIndex)
    #
    # TopAuthorHIndex = AuthorHIndex \
    #     .select(AuthorHIndex.DisplayName, AuthorHIndex.AffiliationDisplayName, AuthorHIndex.PaperCount,
    #             AuthorHIndex.TotalEstimatedCitation, AuthorHIndex.HIndex) \
    #     .orderBy(F.desc('HIndex')) \
    #     .limit(100)
    # print("TopAuthorHIndex.show(50):")
    # TopAuthorHIndex.show(50)
