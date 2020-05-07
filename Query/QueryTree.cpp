/*=============================================================================
# Filename: QueryTree.cpp
# Author: Jiaqi, Chen
# Mail: chenjiaqi93@163.com
# Last Modified: 2017-03-13
# Description: implement functions in QueryTree.h
=============================================================================*/

#include "QueryTree.h"

using namespace std;

void QueryTree::GroupPattern::FilterTree::FilterTreeNode::getVarset(Varset &varset)
{
	for (int i = 0; i < (int)this->child.size(); i++)
	{
		if (this->child[i].node_type == FilterTreeChild::String_type && this->child[i].str[0] == '?')
			varset.addVar(this->child[i].str);
		if (this->child[i].node_type == FilterTreeChild::Tree_type)
			this->child[i].node.getVarset(varset);
	}
}

void QueryTree::GroupPattern::FilterTree::FilterTreeNode::mapVarPos2Varset(Varset &varset, Varset &entity_literal_varset)
{
	if (this->oper_type == Not_type)
	{
		this->child[0].node.mapVarPos2Varset(varset, entity_literal_varset);
	}
	else if (this->oper_type == Or_type || this->oper_type == And_type)
	{
		this->child[0].node.mapVarPos2Varset(varset, entity_literal_varset);
		this->child[1].node.mapVarPos2Varset(varset, entity_literal_varset);
	}
	else if (Equal_type <= this->oper_type && this->oper_type <= GreaterOrEqual_type)
	{
		if (this->child[0].node_type == FilterTreeChild::Tree_type)
			this->child[0].node.mapVarPos2Varset(varset, entity_literal_varset);
		else if (this->child[0].node_type == FilterTreeChild::String_type && this->child[0].str[0] == '?')
		{
			this->child[0].pos = Varset(this->child[0].str).mapTo(varset)[0];
			this->child[0].isel = entity_literal_varset.findVar(this->child[0].str);
		}

		if (this->child[1].node_type == FilterTreeChild::Tree_type)
			this->child[1].node.mapVarPos2Varset(varset, entity_literal_varset);
		else if (this->child[1].node_type == FilterTreeChild::String_type && this->child[1].str[0] == '?')
		{
			this->child[1].pos = Varset(this->child[1].str).mapTo(varset)[0];
			this->child[1].isel = entity_literal_varset.findVar(this->child[1].str);
		}
	}
	else if (this->oper_type == Builtin_regex_type ||
			 this->oper_type == Builtin_str_type ||
			 this->oper_type == Builtin_isiri_type ||
			 this->oper_type == Builtin_isuri_type ||
			 this->oper_type == Builtin_isliteral_type ||
			 this->oper_type == Builtin_isnumeric_type ||
			 this->oper_type == Builtin_lang_type ||
			 this->oper_type == Builtin_langmatches_type ||
			 this->oper_type == Builtin_bound_type ||
			 this->oper_type == Builtin_in_type)
	{
		if (this->child[0].node_type == FilterTreeChild::Tree_type)
			this->child[0].node.mapVarPos2Varset(varset, entity_literal_varset);
		else if (this->child[0].node_type == FilterTreeChild::String_type && this->child[0].str[0] == '?')
		{
			this->child[0].pos = Varset(this->child[0].str).mapTo(varset)[0];
			this->child[0].isel = entity_literal_varset.findVar(this->child[0].str);
		}
	}
}

void QueryTree::GroupPattern::FilterTree::FilterTreeNode::print(int dep)
{
	if (this->oper_type == Not_type)					printf("!");
	if (this->oper_type == Builtin_regex_type)			printf("REGEX");
	if (this->oper_type == Builtin_str_type)			printf("STR");
	if (this->oper_type == Builtin_isiri_type)			printf("ISIRI");
	if (this->oper_type == Builtin_isuri_type)			printf("ISURI");
	if (this->oper_type == Builtin_isliteral_type)		printf("ISLITERAL");
	if (this->oper_type == Builtin_isnumeric_type)		printf("ISNUMERIC");
	if (this->oper_type == Builtin_lang_type)			printf("LANG");
	if (this->oper_type == Builtin_langmatches_type)	printf("LANGMATCHES");
	if (this->oper_type == Builtin_bound_type)			printf("BOUND");

	if (this->oper_type == Builtin_in_type)
	{
		printf("(");
		if (this->child[0].node_type == FilterTreeChild::String_type)	printf("%s", this->child[0].str.c_str());
		if (this->child[0].node_type == FilterTreeChild::Tree_type)		this->child[0].node.print(dep);
		printf(" IN (");
		for (int i = 1; i < (int)this->child.size(); i++)
		{
			if (i != 1)	printf(", ");
			if (this->child[i].node_type == FilterTreeChild::String_type)	printf("%s", this->child[i].str.c_str());
			if (this->child[i].node_type == FilterTreeChild::Tree_type)		this->child[i].node.print(dep);
		}
		printf("))");

		return;
	}

	printf("(");

	if ((int)this->child.size() >= 1)
	{
		if (this->child[0].node_type == FilterTreeChild::String_type)	printf("%s", this->child[0].str.c_str());
		if (this->child[0].node_type == FilterTreeChild::Tree_type)		this->child[0].node.print(dep);
	}

	if (this->oper_type == Or_type)				printf(" || ");
	if (this->oper_type == And_type)			printf(" && ");
	if (this->oper_type == Equal_type)			printf(" = ");
	if (this->oper_type == NotEqual_type)		printf(" != ");
	if (this->oper_type == Less_type)			printf(" < ");
	if (this->oper_type == LessOrEqual_type)	printf(" <= ");
	if (this->oper_type == Greater_type)		printf(" > ");
	if (this->oper_type == GreaterOrEqual_type)	printf(" >= ");

	if (this->oper_type == Builtin_regex_type || this->oper_type == Builtin_langmatches_type)	printf(", ");

	if ((int)this->child.size() >= 2)
	{
		if (this->child[1].node_type == FilterTreeChild::String_type)	printf("%s", this->child[1].str.c_str());
		if (this->child[1].node_type == FilterTreeChild::Tree_type)		this->child[1].node.print(dep);
	}

	if ((int)this->child.size() >= 3)
	{
		if (this->oper_type == FilterTreeNode::Builtin_regex_type && this->child[2].node_type == FilterTreeChild::String_type)
			printf(", %s", this->child[2].str.c_str());
	}

	printf(")");
}

//----------------------------------------------------------------------------------------------------------------------------------------------------

void QueryTree::GroupPattern::addOnePattern(Pattern _pattern)
{
	this->sub_group_pattern.push_back(SubGroupPattern(SubGroupPattern::Pattern_type));
	this->sub_group_pattern.back().pattern = _pattern;
}

void QueryTree::GroupPattern::addOneGroupUnion()
{
	this->sub_group_pattern.push_back(SubGroupPattern(SubGroupPattern::Union_type));
}

void QueryTree::GroupPattern::addOneUnion()
{
	if (this->sub_group_pattern.back().type != SubGroupPattern::Union_type)
		throw "QueryTree::GroupPattern::addOneUnion failed";

	this->sub_group_pattern.back().unions.push_back(GroupPattern());
}

QueryTree::GroupPattern& QueryTree::GroupPattern::getLastUnion()
{
	if (this->sub_group_pattern.back().type != SubGroupPattern::Union_type || this->sub_group_pattern.back().unions.empty())
		throw "QueryTree::GroupPattern::getLastUnion failed";

	return this->sub_group_pattern.back().unions.back();
}

void QueryTree::GroupPattern::addOneOptional(int _type)
{
	SubGroupPattern::SubGroupPatternType type = (SubGroupPattern::SubGroupPatternType)_type;
	if (type != SubGroupPattern::Optional_type && type != SubGroupPattern::Minus_type)
		throw "QueryTree::GroupPattern::addOneOptional failed";

	this->sub_group_pattern.push_back(SubGroupPattern(type));
}

QueryTree::GroupPattern& QueryTree::GroupPattern::getLastOptional()
{
	if (this->sub_group_pattern.back().type != SubGroupPattern::Optional_type && this->sub_group_pattern.back().type != SubGroupPattern::Minus_type)
		throw "QueryTree::GroupPattern::getLastOptional failed";

	return this->sub_group_pattern.back().optional;
}

void QueryTree::GroupPattern::addOneFilter()
{
	this->sub_group_pattern.push_back(SubGroupPattern(SubGroupPattern::Filter_type));
}

QueryTree::GroupPattern::FilterTree& QueryTree::GroupPattern::getLastFilter()
{
	if (this->sub_group_pattern.back().type != SubGroupPattern::Filter_type)
		throw "QueryTree::GroupPattern::getLastFilter failed";

	return this->sub_group_pattern.back().filter;
}

void QueryTree::GroupPattern::addOneBind()
{
	this->sub_group_pattern.push_back(SubGroupPattern(SubGroupPattern::Bind_type));
}

QueryTree::GroupPattern::Bind& QueryTree::GroupPattern::getLastBind()
{
	if (this->sub_group_pattern.back().type != SubGroupPattern::Bind_type)
		throw "QueryTree::GroupPattern::getLastBind failed";

	return this->sub_group_pattern.back().bind;
}

void QueryTree::GroupPattern::getVarset()
{
	for (int i = 0; i < (int)this->sub_group_pattern.size(); i++)
		if (this->sub_group_pattern[i].type == SubGroupPattern::Pattern_type)
		{
			if (this->sub_group_pattern[i].pattern.subject.value[0] == '?')
			{
				this->sub_group_pattern[i].pattern.varset.addVar(this->sub_group_pattern[i].pattern.subject.value);
				this->sub_group_pattern[i].pattern.subject_object_varset.addVar(this->sub_group_pattern[i].pattern.subject.value);
				this->group_pattern_subject_object_maximal_varset.addVar(this->sub_group_pattern[i].pattern.subject.value);
			}
			if (this->sub_group_pattern[i].pattern.predicate.value[0] == '?')
			{
				this->sub_group_pattern[i].pattern.varset.addVar(this->sub_group_pattern[i].pattern.predicate.value);
				this->group_pattern_predicate_maximal_varset.addVar(this->sub_group_pattern[i].pattern.predicate.value);
			}
			if (this->sub_group_pattern[i].pattern.object.value[0] == '?')
			{
				this->sub_group_pattern[i].pattern.varset.addVar(this->sub_group_pattern[i].pattern.object.value);
				this->sub_group_pattern[i].pattern.subject_object_varset.addVar(this->sub_group_pattern[i].pattern.object.value);
				this->group_pattern_subject_object_maximal_varset.addVar(this->sub_group_pattern[i].pattern.object.value);
			}
			this->group_pattern_resultset_minimal_varset += this->sub_group_pattern[i].pattern.varset;
			this->group_pattern_resultset_maximal_varset += this->sub_group_pattern[i].pattern.varset;
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Union_type)
		{
			Varset minimal_varset;

			for (int j = 0; j < (int)this->sub_group_pattern[i].unions.size(); j++)
			{
				this->sub_group_pattern[i].unions[j].getVarset();
				if (j == 0)
					minimal_varset = this->sub_group_pattern[i].unions[j].group_pattern_resultset_minimal_varset;
				else
					minimal_varset = minimal_varset * this->sub_group_pattern[i].unions[j].group_pattern_resultset_minimal_varset;
				this->group_pattern_resultset_maximal_varset += this->sub_group_pattern[i].unions[j].group_pattern_resultset_maximal_varset;
				this->group_pattern_subject_object_maximal_varset += this->sub_group_pattern[i].unions[j].group_pattern_subject_object_maximal_varset;
				this->group_pattern_predicate_maximal_varset += this->sub_group_pattern[i].unions[j].group_pattern_predicate_maximal_varset;
			}

			this->group_pattern_resultset_minimal_varset += minimal_varset;
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Optional_type)
		{
			this->sub_group_pattern[i].optional.getVarset();

			this->group_pattern_resultset_maximal_varset += this->sub_group_pattern[i].optional.group_pattern_resultset_maximal_varset;
			this->group_pattern_subject_object_maximal_varset += this->sub_group_pattern[i].optional.group_pattern_subject_object_maximal_varset;
			this->group_pattern_predicate_maximal_varset += this->sub_group_pattern[i].optional.group_pattern_predicate_maximal_varset;
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Minus_type)
		{
			this->sub_group_pattern[i].optional.getVarset();
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Filter_type)
		{
			this->sub_group_pattern[i].filter.root.getVarset(this->sub_group_pattern[i].filter.varset);
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Bind_type)
		{
			this->sub_group_pattern[i].bind.varset = Varset(this->sub_group_pattern[i].bind.var);
			this->group_pattern_resultset_minimal_varset += this->sub_group_pattern[i].bind.varset;
			this->group_pattern_resultset_maximal_varset += this->sub_group_pattern[i].bind.varset;
		}
}

pair<Varset, Varset> QueryTree::GroupPattern::checkNoMinusAndOptionalVarAndSafeFilter(Varset occur_varset, Varset ban_varset, bool &check_condition)
//return occur varset and ban varset
{
	if (!check_condition)	return make_pair(Varset(), Varset());

	Varset new_ban_varset;

	for (int i = 0; i < (int)this->sub_group_pattern.size(); i++)
		if (!check_condition)	break;
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Pattern_type)
		{
			if (this->sub_group_pattern[i].pattern.varset.hasCommonVar(ban_varset))
				check_condition = false;

			occur_varset += this->sub_group_pattern[i].pattern.varset;
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Union_type)
		{
			Varset sub_occur_varset, sub_ban_varset;

			for (int j = 0; j < (int)this->sub_group_pattern[i].unions.size(); j++)
			{
				pair<Varset, Varset> sub_return_varset =
					this->sub_group_pattern[i].unions[j].checkNoMinusAndOptionalVarAndSafeFilter(occur_varset, ban_varset, check_condition);

				if (j == 0)
					sub_occur_varset = sub_return_varset.first;
				else
					sub_occur_varset = sub_occur_varset * sub_return_varset.first;

				sub_ban_varset += sub_return_varset.second;
			}

			new_ban_varset += sub_ban_varset;
			occur_varset += sub_occur_varset;
			ban_varset += new_ban_varset;
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Optional_type)
		{
			pair<Varset, Varset> sub_return_varset =
				this->sub_group_pattern[i].optional.checkNoMinusAndOptionalVarAndSafeFilter(Varset(), ban_varset, check_condition);

			//occur before
			if (occur_varset.hasCommonVar(sub_return_varset.second))
				check_condition = false;

			new_ban_varset += sub_return_varset.second;
			new_ban_varset += this->sub_group_pattern[i].optional.group_pattern_resultset_maximal_varset - occur_varset;
			occur_varset += sub_return_varset.first;
			ban_varset += new_ban_varset;
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Minus_type)
		{
			check_condition = false;
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Filter_type)
		{
			if (!this->sub_group_pattern[i].filter.varset.belongTo(occur_varset))
				check_condition = false;
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Bind_type)
		{
			if (this->sub_group_pattern[i].bind.varset.hasCommonVar(ban_varset))
				check_condition = false;

			occur_varset += this->sub_group_pattern[i].bind.varset;
		}

	return make_pair(occur_varset, new_ban_varset);
}

void QueryTree::GroupPattern::initPatternBlockid()
{
	for (int i = 0; i < (int)this->sub_group_pattern.size(); i++)
		if (this->sub_group_pattern[i].type == SubGroupPattern::Pattern_type)
			this->sub_group_pattern[i].pattern.blockid = i;
}

int QueryTree::GroupPattern::getRootPatternBlockID(int x)
{
	if (this->sub_group_pattern[x].type != SubGroupPattern::Pattern_type)
		throw "QueryTree::GroupPattern::getRootPatternBlockID failed";

	if (this->sub_group_pattern[x].pattern.blockid == x)
		return x;
	this->sub_group_pattern[x].pattern.blockid = this->getRootPatternBlockID(this->sub_group_pattern[x].pattern.blockid);

	return this->sub_group_pattern[x].pattern.blockid;
}

void QueryTree::GroupPattern::mergePatternBlockID(int x, int y)
{
	int px = this->getRootPatternBlockID(x);
	int py = this->getRootPatternBlockID(y);
	this->sub_group_pattern[px].pattern.blockid = py;
}

void QueryTree::GroupPattern::print(int dep)
{
	for (int t = 0; t < dep; t++)	printf("\t");	printf("{\n");

	for (int i = 0; i < (int)this->sub_group_pattern.size(); i++)
		if (this->sub_group_pattern[i].type == SubGroupPattern::Pattern_type)
		{
			for (int t = 0; t <= dep; t++)	printf("\t");
			printf("%s\t%s\t%s.\n",	this->sub_group_pattern[i].pattern.subject.value.c_str(),
									this->sub_group_pattern[i].pattern.predicate.value.c_str(),
									this->sub_group_pattern[i].pattern.object.value.c_str());
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Union_type)
		{
			for (int j = 0; j < (int)this->sub_group_pattern[i].unions.size(); j++)
			{
				if (j != 0)
				{
					for (int t = 0; t <= dep; t++)	printf("\t");	printf("UNION\n");
				}
				this->sub_group_pattern[i].unions[j].print(dep + 1);
			}
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Optional_type || this->sub_group_pattern[i].type == SubGroupPattern::Minus_type)
		{
			for (int t = 0; t <= dep; t++)	printf("\t");
			if (this->sub_group_pattern[i].type == SubGroupPattern::Optional_type)	printf("OPTIONAL\n");
			if (this->sub_group_pattern[i].type == SubGroupPattern::Minus_type)	printf("MINUS\n");
			this->sub_group_pattern[i].optional.print(dep + 1);
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Filter_type)
		{
			for (int t = 0; t <= dep; t++)	printf("\t");	printf("FILTER\t");
			this->sub_group_pattern[i].filter.root.print(dep + 1);
			printf("\n");
		}
		else if (this->sub_group_pattern[i].type == SubGroupPattern::Bind_type)
		{
			for (int t = 0; t <= dep; t++)	printf("\t");
			printf("BIND(%s\tAS\t%s)", this->sub_group_pattern[i].bind.str.c_str(), this->sub_group_pattern[i].bind.var.c_str());
			printf("\n");
		}

	for (int t = 0; t < dep; t++)	printf("\t");	printf("}\n");
}

//----------------------------------------------------------------------------------------------------------------------------------------------------

void QueryTree::setQueryForm(QueryForm _queryform)
{
	this->query_form = _queryform;
}

QueryTree::QueryForm QueryTree::getQueryForm()
{
	return this->query_form;
}

void QueryTree::setProjectionModifier(ProjectionModifier _projection_modifier)
{
	this->projection_modifier = _projection_modifier;
}

QueryTree::ProjectionModifier QueryTree::getProjectionModifier()
{
	return this->projection_modifier;
}

void QueryTree::addProjectionVar()
{
	this->projection.push_back(ProjectionVar());
}

QueryTree::ProjectionVar& QueryTree::getLastProjectionVar()
{
	return this->projection.back();
}

vector<QueryTree::ProjectionVar>& QueryTree::getProjection()
{
	return this->projection;
}

Varset QueryTree::getProjectionVarset()
{
	Varset varset;

	for (int i = 0; i < (int)this->projection.size(); i++)
		varset.addVar(this->projection[i].var);

	return varset;
}

Varset QueryTree::getResultProjectionVarset()
{
	Varset varset;

	for (int i = 0; i < (int)this->projection.size(); i++)
		if (this->projection[i].aggregate_type == ProjectionVar::None_type)
			varset.addVar(this->projection[i].var);
		else if (this->projection[i].aggregate_var != "*")
			varset.addVar(this->projection[i].aggregate_var);

	return varset;
}

void QueryTree::setProjectionAsterisk()
{
	this->projection_asterisk = true;
}

bool QueryTree::checkProjectionAsterisk()
{
	return this->projection_asterisk;
}

void QueryTree::addGroupByVar(string &_var)
{
	this->group_by.addVar(_var);
}

Varset& QueryTree::getGroupByVarset()
{
	return this->group_by;
}

void QueryTree::addOrderVar(string &_var, bool _descending)
{
	this->order_by.push_back(Order(_var, _descending));
}

vector<QueryTree::Order>& QueryTree::getOrderVarVector()
{
	return this->order_by;
}

Varset QueryTree::getOrderByVarset()
{
	Varset varset;

	for (int i = 0; i < (int)this->order_by.size(); i++)
		varset.addVar(this->order_by[i].var);

	return varset;
}

void QueryTree::setOffset(int _offset)
{
	this->offset = _offset;
}

int QueryTree::getOffset()
{
	return this->offset;
}

void QueryTree::setLimit(int _limit)
{
	this->limit = _limit;
}

int QueryTree::getLimit()
{
	return this->limit;
}

QueryTree::GroupPattern& QueryTree::getGroupPattern()
{
	return this->group_pattern;
}

void QueryTree::setUpdateType(UpdateType _updatetype)
{
	this->update_type = _updatetype;
}

QueryTree::UpdateType QueryTree::getUpdateType()
{
	return this->update_type;
}

QueryTree::GroupPattern& QueryTree::getInsertPatterns()
{
	return this->insert_patterns;
}

QueryTree::GroupPattern& QueryTree::getDeletePatterns()
{
	return this->delete_patterns;
}

bool QueryTree::checkWellDesigned()
{
	bool check_condition = true;
	this->group_pattern.checkNoMinusAndOptionalVarAndSafeFilter(Varset(), Varset(), check_condition);
	return check_condition;
}

bool QueryTree::checkAtLeastOneAggregateFunction()
{
	for (int i = 0; i < (int)this->projection.size(); i++)
		if (this->projection[i].aggregate_type != ProjectionVar::None_type)
			return true;

	return false;
}

bool QueryTree::checkSelectAggregateFunctionGroupByValid()
{
	if (this->checkAtLeastOneAggregateFunction() && this->group_by.empty())
	{
		for (int i = 0; i < (int)this->projection.size(); i++)
			if (this->projection[i].aggregate_type == ProjectionVar::None_type)
				return false;
	}

	if (!this->group_by.empty())
	{
		for (int i = 0; i < (int)this->projection.size(); i++)
			if (this->projection[i].aggregate_type == ProjectionVar::None_type && !this->group_by.findVar(this->projection[i].var))
				return false;
	}

	return true;
}

void 
QueryTree::getWholeQuery(string& _query)
{
	vector<QueryTree::GroupPattern::SubGroupPattern> p_vec = this->getGroupPattern().sub_group_pattern;
	set<string>queryPoint;
	string sparql="";
	for(int i = 0; i < p_vec.size(); i++)
	{
		if(p_vec[i].pattern.subject.value[0]=='?')
			queryPoint.insert(p_vec[i].pattern.subject.value);
		if(p_vec[i].pattern.object.value[0]=='?')
			queryPoint.insert(p_vec[i].pattern.object.value);
		if(p_vec[i].pattern.predicate.value[0]=='?')
			queryPoint.insert(p_vec[i].pattern.predicate.value);
		sparql+=p_vec[i].pattern.subject.value+"\t"+p_vec[i].pattern.predicate.value+"\t"+p_vec[i].pattern.object.value+" .\n";
	}
	string start="select ";
	for(set<string>::iterator it=queryPoint.begin();it!=queryPoint.end();it++)start+=*it+" ";
	_query=start+"where {\n"+sparql+"}";
}
<<<<<<< HEAD
int 
QueryTree::checkStar(vector<decompose_query_result>& result,set<string>& crossingEdge,unordered_map<string,int>& edge_cnt)
=======
bool 
QueryTree::checkStar(vector<decompose_query_result>& result,set<string>& crossingEdge,map<string,int>& edge_cnt)
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
{
	vector<QueryTree::GroupPattern::SubGroupPattern> p_vec = this->getGroupPattern().sub_group_pattern;
	
	if(p_vec.size() <= 1)
<<<<<<< HEAD
		return 1;
=======
		return true;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
	
	string center_var="";
	if(p_vec[0].pattern.subject.value.compare(p_vec[1].pattern.subject.value) == 0 || p_vec[0].pattern.subject.value.compare(p_vec[1].pattern.object.value) == 0){
		center_var = p_vec[0].pattern.subject.value;
	}else if(p_vec[0].pattern.object.value.compare(p_vec[1].pattern.subject.value) == 0 || p_vec[0].pattern.object.value.compare(p_vec[1].pattern.object.value) == 0){
		center_var = p_vec[0].pattern.object.value;
	}
	
	int flag=1;
	for(int i = 2; i < p_vec.size(); i++){
		if(p_vec[i].pattern.subject.value.compare(center_var) != 0 && p_vec[i].pattern.object.value.compare(center_var) != 0){
			flag=0;
			break;
		}
	}
<<<<<<< HEAD
	if(flag)return 1;
=======
	if(flag)return true;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8

	map<string,int>valueToID;
	int ID=0;
	for(int i=0;i<p_vec.size();i++)
	{
		if(valueToID.count(p_vec[i].pattern.subject.value)==0)
			valueToID[p_vec[i].pattern.subject.value]=++ID;
		if(valueToID.count(p_vec[i].pattern.object.value)==0)
			valueToID[p_vec[i].pattern.object.value]=++ID;
	}

	vector<int>fa(ID+1);
	vector<int>d(ID+1,0);
	vector<vector<int> >edge(ID+1,vector<int>());
	for(int i=1;i<=ID;i++)fa[i]=i;
	for(int i=0;i<p_vec.size();i++)
	{

		int faA=valueToID[p_vec[i].pattern.subject.value];
		int faB=valueToID[p_vec[i].pattern.object.value];
		edge[faA].push_back(faB);
		edge[faB].push_back(faA);
		if(p_vec[i].pattern.predicate.value[0]!='?'&&crossingEdge.count(p_vec[i].pattern.predicate.value)==0)
		{
			d[faA]++;d[faB]++;
			while(faA!=fa[faA])faA=fa[faA];
			while(faB!=fa[faB])faB=fa[faB];
			if(faA>faB)swap(faA,faB);
			fa[faB]=faA;
		}
	}
	int p=1;flag=1;
	for(int i=1;i<=ID;i++)if(fa[i]==i&&d[i]){p=i;break;}
	for(int i=1;i<=ID;i++)
	{
		int pa=i;
		while(pa!=fa[pa])pa=fa[pa];
		if(pa!=p)
		{
			if(d[i]){flag=0;break;}
			for(int j=0;j<edge[i].size();j++)
			{
				pa=edge[i][j];
				while(pa!=fa[pa])pa=fa[pa];
				if(pa!=p){flag=0;break;}
			}
		}
		if(!flag)break;
	}
<<<<<<< HEAD
	if(flag)return -1;
=======
	if(flag)return true;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8

	vector<int> IDToBlock(ID+1);
	int block=0;
	for(int i=1;i<=ID;i++)if(fa[i]==i)IDToBlock[i]=++block;
	result=vector<decompose_query_result>(block+1);
	vector<int>block_size(block+1,2147483647);
	// cout<<block<<endl;
	for(int i=0;i<p_vec.size();i++)
	{
		int A=valueToID[p_vec[i].pattern.subject.value];
		int B=valueToID[p_vec[i].pattern.object.value];
		while(A!=fa[A])A=fa[A];
		while(B!=fa[B])B=fa[B];
		A=IDToBlock[A];
		B=IDToBlock[B];
		if(p_vec[i].pattern.subject.value[0]!='?')block_size[A]=1;
		if(p_vec[i].pattern.object.value[0]!='?')block_size[B]=1;
		// cout<<A<<" "<<B<<" "<<block_size[A]<<" "<<block_size[B]<<endl;
		if(A!=B)
		{
			string connection_point="";
			if(block_size[A]>=block_size[B])
			{
				result[A].edge.push_back(p_vec[i].pattern.subject.value+"\t"+p_vec[i].pattern.predicate.value+"\t"+p_vec[i].pattern.object.value);
				connection_point=p_vec[i].pattern.object.value;
				block_size[A]=min(block_size[A],edge_cnt[p_vec[i].pattern.predicate.value]);
			}
			else
			{
				result[B].edge.push_back(p_vec[i].pattern.subject.value+"\t"+p_vec[i].pattern.predicate.value+"\t"+p_vec[i].pattern.object.value);
				connection_point=p_vec[i].pattern.subject.value;
				block_size[B]=min(block_size[B],edge_cnt[p_vec[i].pattern.predicate.value]);
			}
			
			result[A].connection.push_back(make_pair(connection_point,B));
			result[B].connection.push_back(make_pair(connection_point,A));
		}
		else
		{
			result[A].edge.push_back(p_vec[i].pattern.subject.value+"\t"+p_vec[i].pattern.predicate.value+"\t"+p_vec[i].pattern.object.value);
			block_size[A]=min(block_size[A],edge_cnt[p_vec[i].pattern.predicate.value]);
		}
	}
	vector<int> new_block(block+1,0);
	int x=0,y=0;
	vector<decompose_query_result>::iterator it=result.begin();
	// cout<<result.size()<<endl;
	for(it++;it!=result.end();)
	{
		x++;
		if(it->edge.size())new_block[x]=++y,it++;
		else it=result.erase(it);
	}
	// cout<<y<<endl;
	for(int i=1;i<=y;i++)
	{
		for(vector<pair<string,int> >::iterator it=result[i].connection.begin();it!=result[i].connection.end();)
			if(new_block[it->second])
				it->second=new_block[it->second],it++;
			else
				it=result[i].connection.erase(it);

	}
<<<<<<< HEAD
	return 0;
=======
	return false;
>>>>>>> 0348316ba2f9c5895efe4549fd4dc256904c95a8
}

bool 
QueryTree::checkStar(vector<decompose_query_result>& result)
{
	vector<QueryTree::GroupPattern::SubGroupPattern> p_vec = this->getGroupPattern().sub_group_pattern;
	
	if(p_vec.size() <= 1)
		return true;
	
	string center_var="";
	if(p_vec[0].pattern.subject.value.compare(p_vec[1].pattern.subject.value) == 0 || p_vec[0].pattern.subject.value.compare(p_vec[1].pattern.object.value) == 0){
		center_var = p_vec[0].pattern.subject.value;
	}else if(p_vec[0].pattern.object.value.compare(p_vec[1].pattern.subject.value) == 0 || p_vec[0].pattern.object.value.compare(p_vec[1].pattern.object.value) == 0){
		center_var = p_vec[0].pattern.object.value;
	}
	
	int flag=1;
	for(int i = 2; i < p_vec.size(); i++){
		if(p_vec[i].pattern.subject.value.compare(center_var) != 0 && p_vec[i].pattern.object.value.compare(center_var) != 0){
			flag=0;
			break;
		}
	}
	if(flag)return true;

	map<string,int>valueToID;
	map<int,string>IDToValue;
	int ID=0;
	for(int i=0;i<p_vec.size();i++)
	{
		if(valueToID.count(p_vec[i].pattern.subject.value)==0)
		{
			valueToID[p_vec[i].pattern.subject.value]=++ID;
			IDToValue[ID]=p_vec[i].pattern.subject.value;
		}
		if(valueToID.count(p_vec[i].pattern.object.value)==0)
		{
			valueToID[p_vec[i].pattern.object.value]=++ID;
			IDToValue[ID]=p_vec[i].pattern.object.value;	
		}
	}

	vector<int>degree(ID+1,0);
	vector<vector<pair<int,int> > >edge(ID+1,vector<pair<int,int> >());
	for(int i=0;i<p_vec.size();i++)
	{
		degree[valueToID[p_vec[i].pattern.subject.value]]++;
		edge[valueToID[p_vec[i].pattern.subject.value]].push_back(make_pair(valueToID[p_vec[i].pattern.object.value],i));
		degree[valueToID[p_vec[i].pattern.object.value]]++;
		edge[valueToID[p_vec[i].pattern.object.value]].push_back(make_pair(valueToID[p_vec[i].pattern.subject.value],i));
	}

	decompose_query_result tmp;
	result.push_back(tmp);
	vector<vector<int> >IDToBlock(ID+1,vector<int>());
	int block=0;
	while(true)
	{
		int mx=0,p=-1;
		for(int i=1;i<=ID;i++)if(mx<degree[i])mx=degree[i],p=i;
		if(p==-1)break;
		// cout<<p<<endl;
		block++;
		result.push_back(tmp);
		for(int i=0;i<IDToBlock[p].size();i++)
		{
			result[block].connection.push_back(make_pair(IDToValue[p],IDToBlock[p][i]));
			result[IDToBlock[p][i]].connection.push_back(make_pair(IDToValue[p],block));
		}
		IDToBlock[p].push_back(block);

		for(int i=0;i<edge[p].size();i++)
		{
			int v=edge[p][i].first;
			if(!degree[v])continue;
			for(int j=0;j<IDToBlock[v].size();j++)
			{
				result[block].connection.push_back(make_pair(IDToValue[v],IDToBlock[v][j]));
				result[IDToBlock[v][j]].connection.push_back(make_pair(IDToValue[v],block));
			}
			IDToBlock[v].push_back(block);
			degree[p]--;
			degree[v]--;
			int w=edge[p][i].second;
			result[block].edge.push_back(p_vec[w].pattern.subject.value+"\t"+p_vec[w].pattern.predicate.value+"\t"+p_vec[w].pattern.object.value);
		}
	}
	return false;
}

bool 
QueryTree::checkStar_VP(vector<decompose_query_result>& result)
{
	vector<QueryTree::GroupPattern::SubGroupPattern> p_vec = this->getGroupPattern().sub_group_pattern;

	map<string,int>valueToID;
	int ID=0;
	for(int i=0;i<p_vec.size();i++)
	{
		if(valueToID.count(p_vec[i].pattern.subject.value)==0)
		{
			valueToID[p_vec[i].pattern.subject.value]=++ID;
		}
		if(valueToID.count(p_vec[i].pattern.object.value)==0)
		{
			valueToID[p_vec[i].pattern.object.value]=++ID;
		}
	}

	result=vector<decompose_query_result>(p_vec.size()+1);
	vector<vector<int> > IDToBlock(ID+1,vector<int>());	
	for(int i=0;i<p_vec.size();i++)
	{
		result[i+1].edge.push_back(p_vec[i].pattern.subject.value+"\t"+p_vec[i].pattern.predicate.value+"\t"+p_vec[i].pattern.object.value);
		IDToBlock[valueToID[p_vec[i].pattern.subject.value]].push_back(i+1);
		IDToBlock[valueToID[p_vec[i].pattern.object.value]].push_back(i+1);
	}
	for(int i=0;i<p_vec.size();i++)
	{
		int _sub=valueToID[p_vec[i].pattern.subject.value];
		int _obj=valueToID[p_vec[i].pattern.object.value];
		for(int j=0;j<IDToBlock[_sub].size();j++)if(IDToBlock[_sub][j]!=i+1)
		{
			result[i+1].connection.push_back(make_pair(p_vec[i].pattern.subject.value,IDToBlock[_sub][j]));
		}
		for(int j=0;j<IDToBlock[_obj].size();j++)if(IDToBlock[_obj][j]!=i+1)
		{
			result[i+1].connection.push_back(make_pair(p_vec[i].pattern.object.value,IDToBlock[_obj][j]));
		}
	}
	return false;
}

void QueryTree::print()
{
	for (int j = 0; j < 80; j++)			printf("=");	printf("\n");

	if (this->update_type == Not_Update)
	{
		if (this->query_form == Select_Query)
		{
			printf("SELECT");
			if (this->projection_modifier == Modifier_Distinct)
				printf(" DISTINCT");
			printf("\n");

			printf("Var: \t");
			for (int i = 0; i < (int)this->projection.size(); i++)
			{
				if (this->projection[i].aggregate_type == QueryTree::ProjectionVar::None_type)
					printf("%s\t", this->projection[i].var.c_str());
				else
				{
					printf("(");
					if (this->projection[i].aggregate_type == QueryTree::ProjectionVar::Count_type)
						printf("COUNT(");
					if (this->projection[i].aggregate_type == QueryTree::ProjectionVar::Sum_type)
						printf("SUM(");
					if (this->projection[i].aggregate_type == QueryTree::ProjectionVar::Min_type)
						printf("MIN(");
					if (this->projection[i].aggregate_type == QueryTree::ProjectionVar::Max_type)
						printf("MAX(");
					if (this->projection[i].aggregate_type == QueryTree::ProjectionVar::Avg_type)
						printf("AVG(");
					if (this->projection[i].distinct)
						printf("DISTINCT ");
					printf("%s) AS %s)\t", this->projection[i].aggregate_var.c_str(), this->projection[i].var.c_str());
				}
			}
			if (this->projection_asterisk && !this->checkAtLeastOneAggregateFunction())
				printf("*");
			printf("\n");
		}
		else printf("ASK\n");

		printf("GroupPattern:\n");
		this->group_pattern.print(0);

		if (this->query_form == Select_Query)
		{
			if (!this->group_by.empty())
			{
				printf("GROUP BY\t");

				for (int i = 0; i < (int)this->group_by.vars.size(); i++)
					printf("%s\t", this->group_by.vars[i].c_str());

				printf("\n");
			}

			if (!this->order_by.empty())
			{
				printf("ORDER BY\t");

				for (int i = 0; i < (int)this->order_by.size(); i++)
				{
					if (!this->order_by[i].descending)	printf("ASC(");
					else printf("DESC(");
					printf("%s)\t", this->order_by[i].var.c_str());
				}
				printf("\n");
			}
			if (this->offset != 0)
				printf("OFFSET\t%d\n", this->offset);
			if (this->limit != -1)
				printf("LIMIT\t%d\n", this->limit);
		}
	}
	else
	{
		printf("UPDATE\n");
		if (this->update_type == Delete_Data || this->update_type == Delete_Where ||
				this->update_type == Delete_Clause || this->update_type == Modify_Clause)
		{
			printf("Delete:\n");
			this->delete_patterns.print(0);
		}
		if (this->update_type == Insert_Data || this->update_type == Insert_Clause || this->update_type == Modify_Clause)
		{
			printf("Insert:\n");
			this->insert_patterns.print(0);
		}
		if (this->update_type == Delete_Where || this->update_type == Insert_Clause ||
				this->update_type == Delete_Clause || this->update_type == Modify_Clause)
		{
			printf("GroupPattern:\n");
			this->group_pattern.print(0);
		}
	}

	for (int j = 0; j < 80; j++)			printf("=");	printf("\n");
}
