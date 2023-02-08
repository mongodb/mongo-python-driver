class PipelineHelper:
    @staticmethod
    def project(field_mapping):
        """
        Transforms the documents by adding, removing, or modifying fields.
        The 'field_mapping' parameter should be a dictionary that specifies the desired transformations.
        In MongoDB, the '$project' stage is used to reshape the documents in a collection.
        """
        return {"$project": field_mapping}

    @staticmethod
    def match(criteria):
        """
        Filters the documents based on the specified criteria.
        The 'criteria' parameter should be a dictionary that defines the filter conditions.
        In MongoDB, the '$match' stage is used to filter the documents in a collection based on specific conditions.
        """
        return {"$match": criteria}

    @staticmethod
    def group(grouping, calculated_fields):
        """
        Groups the documents based on the specified grouping criteria and calculates aggregated values.
        The 'grouping' parameter should be a dictionary that defines the grouping criteria.
        The 'calculated_fields' parameter should be a dictionary that defines the aggregated values to be calculated.
        In MongoDB, the '$group' stage is used to group the documents in a collection based on specific fields and calculate aggregated values.
        """
        return {"$group": {**grouping, **calculated_fields}}

    @staticmethod
    def sort(sort_order):
        """
        Sorts the documents based on the specified sort order.
        The 'sort_order' parameter should be a dictionary that defines the sort order for the documents.
        In MongoDB, the '$sort' stage is used to sort the documents in a collection based on specific fields.
        """
        return {"$sort": sort_order}

    @staticmethod
    def skip(skip_count):
        """
        Skips a specified number of documents.
        The 'skip_count' parameter should be an integer that specifies the number of documents to skip.
        In MongoDB, the '$skip' stage is used to skip a specified number of documents in a collection.
        """
        return {"$skip": skip_count}

    @staticmethod
    def limit(limit_count):
        """
        Limits the number of documents returned.
        The 'limit_count' parameter should be an integer that specifies the maximum number of documents to return.
        In MongoDB, the '$limit' stage is used to limit the number of documents returned from a collection.
        """
        return {"$limit": limit_count}

    @staticmethod
    def first():
        """
        Returns the first document from the documents returned by the previous stages.
        In MongoDB, the '$first' expression is used to return the first document from an array of documents.
        """
        return {"$first": "$$ROOT"}

    @staticmethod
    def last():
        """
        Returns the last document from the documents returned by the previous stages.
        In MongoDB, the '$last' expression is used to return the last document from an array of documents.
        """
        return {"$last": "$$ROOT"}

    @staticmethod
    def unwind(field):
        """
        Decomposes an array field from the input documents and returns output documents with one output document for each element in the specified array field.
        The unwind stage can be useful in several scenarios such as when you want to perform aggregation operations on individual elements of an array or when 
        you want to denormalize arrays into separate documents to make querying and filtering easier.
        """
        return {"$unwind": field}
