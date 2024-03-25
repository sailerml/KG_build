import os
import pdb
import re
import json
import codecs
import threading
from py2neo import Graph
import pandas as pd
import numpy as np
from tqdm import tqdm
import xlrd


pattern2=re.compile(r'(\()(.*?)(\))')       #提取括号中的字符

def load_kg_excel(basegraph_path, relarre_path, nodeattr_path):

    #s1：首先加载实体与关系总框架表
    workbook = xlrd.open_workbook(basegraph_path)
    ent_info_name = workbook.sheet_names()[0]
    pp_info_name = workbook.sheet_names()[1]
    ps_info_name = workbook.sheet_names()[2]
    pres_info_name = workbook.sheet_names()[3]
    foodall_info_name = workbook.sheet_names()[4]
    acupall_info_name = workbook.sheet_names()[5]


    #s2：加载节点属性
    nodeattr_workbook = xlrd.open_workbook(nodeattr_path)
    symattr_info_name = nodeattr_workbook.sheet_names()[0]
    fomuattr_info_name = nodeattr_workbook.sheet_names()[1]
    food_info_name = nodeattr_workbook.sheet_names()[2]
    acup_info_name = nodeattr_workbook.sheet_names()[3]

    #s3：加载关系属性
    relworkbook = xlrd.open_workbook(relarre_path)
    presrel_info_name = relworkbook.sheet_names()[0]
    pprel_info_name = relworkbook.sheet_names()[1]
    psrel_info_name = relworkbook.sheet_names()[2]

    # 1. 实体列表
    entity_dic = {
        '症状': [         # {'name':'', 'attr':{}}
        ],
        '病机':[],
        '处方':[
        ],
        '膳食':[
        ],
        '穴位':[
        ],
        '方药':[],
    }
    ent_info_sheet = workbook.sheet_by_name(ent_info_name)
    symptom_lis = ent_info_sheet.col_values(0)[1:]
    pathogenesis_lis = ent_info_sheet.col_values(2)[1:]
    formula_lis = ent_info_sheet.col_values(4)[1:]
    #prescription_lis = ent_info_sheet.col_values(6)[1:]
    food_lis = ent_info_sheet.col_values(8)[1:]
    acupuncture_lis = ent_info_sheet.col_values(10)[1:]

    # 读取症状节点的属性
    symattr_info_sheet = nodeattr_workbook.sheet_by_name(symattr_info_name)
    symattr_symlis = symattr_info_sheet.col_values(0)[1:]
    symattr_catlis = symattr_info_sheet.col_values(1)[1:]
    symattr_deslis = symattr_info_sheet.col_values(2)[1:]

    # 读取处方属性
    fomuattr_info_sheet = nodeattr_workbook.sheet_by_name(fomuattr_info_name)
    fomuattr_fomulis = fomuattr_info_sheet.col_values(0)[1:]
    fomuattr_uselis = fomuattr_info_sheet.col_values(1)[1:]

    # 读取膳食属性
    foodattr_info_sheet = nodeattr_workbook.sheet_by_name(food_info_name)
    foodattr_foodlis = foodattr_info_sheet.col_values(0)[1:]
    foodattr_dosalis = foodattr_info_sheet.col_values(1)[1:]
    foodattr_proclis = foodattr_info_sheet.col_values(2)[1:]
    foodattr_edilis = foodattr_info_sheet.col_values(3)[1:]

    #读取穴位属性
    acupattr_info_sheet = nodeattr_workbook.sheet_by_name(acup_info_name)
    acupattr_acuplis = acupattr_info_sheet.col_values(0)[1:]
    acupattr_opelis = acupattr_info_sheet.col_values(1)[1:]



    def load_lis(inlis, dicname):
        for item in inlis:
            item = item.strip()
            if item != '' and item not in [dic['name'] for dic in entity_dic[dicname]]:
                tmpdic = {'name': item, 'attr':{}}
                entity_dic[dicname].append(tmpdic)
        print(dicname, ' has :', len(entity_dic[dicname]))


    load_lis(symptom_lis, '症状')

    for symattr_sym, symattr_cat, symattr_des in zip(symattr_symlis, symattr_catlis, symattr_deslis):
        symattr_sym = symattr_sym.strip()
        symattr_cat = symattr_cat.strip()
        symattr_des = symattr_des.strip()
        for dic in entity_dic['症状']:
            if dic['name'] == symattr_sym:
                dic['attr']['category'] = symattr_cat
                dic['attr']['des'] = symattr_des

    load_lis(pathogenesis_lis, '病机')

    load_lis(formula_lis, '处方')
    for fomuattr_fomu, fomuattr_use in zip(fomuattr_fomulis, fomuattr_uselis):

        fomuattr_fomu = fomuattr_fomu.strip()
        fomuattr_use = fomuattr_use.strip()
        for dic in entity_dic['处方']:
            if dic['name'] == fomuattr_fomu:
                dic['attr']['useage'] = fomuattr_use


    load_lis(food_lis, '膳食')
    for foodattr_food, foodattr_dosa, foodattr_proc, foodattr_edi in zip(foodattr_foodlis, foodattr_dosalis, foodattr_proclis, foodattr_edilis):

        foodattr_food = foodattr_food.strip()
        foodattr_dosa = foodattr_dosa.strip()
        foodattr_proc = foodattr_proc.strip()
        foodattr_edi = foodattr_edi.strip()
        for dic in entity_dic['膳食']:
            if dic['name'] == foodattr_food:
                dic['attr']['dosage'] = foodattr_dosa
                dic['attr']['production'] = foodattr_proc
                dic['attr']['edible'] = foodattr_edi


    load_lis(acupuncture_lis, '穴位')

    for acupattr_acup, acupattr_ope in zip(acupattr_acuplis, acupattr_opelis):
        acupattr_acup = acupattr_acup.strip()
        acupattr_ope = acupattr_ope.strip()
        for dic in entity_dic['穴位']:
            if dic['name'] == acupattr_acup:
                dic['attr']['operating'] = acupattr_ope


    relation_dic = {
        'path_to_path':{
            'head_type':'病机',
            'tail_type': '病机',
            'list': []
        },
        'path_to_sym': {
            'head_type':'病机',
            'tail_type': '症状',
            'list': []
        },
        'prescribe': {
            'head_type':'病机',
            'tail_type': '处方',
            'list': []
        },
        'include': {
            'head_type': '处方',
            'tail_type': '方药',
            'list': []
        },
        'do_eat': {
            'head_type': '病机',
            'tail_type': '膳食',
            'list': []  # [startpa, dicname, endpa, procs],procs为该关系的属性
        },
        'do_acupunc': {
            'head_type': '病机',
            'tail_type': '穴位',
            'list': []  # [startpa, dicname, endpa, procs],procs为该关系的属性
        },
    }

    #读取处方中包含的药方，此处同时Load方药实体，以及处方与方药关系，还有属性

    presrel_info_sheet = relworkbook.sheet_by_name(presrel_info_name)
    formula_lis = presrel_info_sheet.col_values(0)[1:]
    pres_lis = presrel_info_sheet.col_values(1)[1:]

    pprel_info_sheet = relworkbook.sheet_by_name(pprel_info_name)
    pprel_splis = pprel_info_sheet.col_values(0)[1:]
    pprel_eplis = pprel_info_sheet.col_values(1)[1:]
    pprel_caulis = pprel_info_sheet.col_values(2)[1:]

    psrel_info_sheet = relworkbook.sheet_by_name(psrel_info_name)
    psrel_symlis = psrel_info_sheet.col_values(0)[1:]
    psrel_pathlis = psrel_info_sheet.col_values(1)[1:]
    psrel_orilis = psrel_info_sheet.col_values(2)[1:]
    psrel_modlis = psrel_info_sheet.col_values(3)[1:]

    for formu, pres in zip(formula_lis, pres_lis):
        formu = formu.strip()
        if '、' not in pres:
            presarr = pres.strip().replace('（','(').replace('）',')').split('\u3000')
        else:
            presarr = pres.strip().replace('（', '(').replace('）', ')').split('、')

        for prea in presarr:
            presname = prea.split('(')[0].strip()
            presunit = ''
            preprocess = ''
            try:
                presunit = pattern2.search(prea).group(2)
            except:
                print(prea, ' do not have unit !')

            preprocess = prea.split(')')[-1].strip()
            if presname != '' and presname not in [dic['name'] for dic in entity_dic['方药']]:

                tmpdic = {'name':presname, 'attr':{}}
                entity_dic['方药'].append(tmpdic)
            procs = {}
            if presunit != '':
                procs['unit'] = presunit
            if preprocess != '':

                procs['prepro'] = preprocess
            tmprel = [formu, 'include', presname, procs]
            relation_dic['include']['list'].append(tmprel)

    print('方药 has :', len(entity_dic['方药']))
    print('include relation : ', len(relation_dic['include']['list']))


    def construct_rel(inlis, dicname):
        startpa = inlis[0].strip()
        endpath = inlis[1:]
        proc = {}
        for endpa in endpath:
            endpa = endpa.strip()
            if endpa != '':
                if dicname == 'do_acupunc':
                    acupuncs = endpa.split('、')
                    for acupunc in acupuncs:
                        acupunc = acupunc.strip()
                        if acupunc != '':
                            rel = [startpa, dicname, acupunc, proc]
                            if rel not in relation_dic[dicname]['list']:
                                relation_dic[dicname]['list'].append(rel)
                else:
                    rel = [startpa, dicname, endpa, proc]
                    if rel not in relation_dic[dicname]['list']:
                        relation_dic[dicname]['list'].append(rel)



    # 2. 证证关联
    # pp_info_sheet = workbook.sheet_by_name(pp_info_name)
    # for i in range(0, 15, 2):
    #     tmp_lis = pp_info_sheet.col_values(i)[0:]
    #     construct_rel(tmp_lis, 'path_to_path')

    for pprel_sp, pprel_ep, pprel_cau in zip(pprel_splis, pprel_eplis, pprel_caulis):
        pprel_sp = pprel_sp.strip()
        pprel_ep = pprel_ep.strip()
        pprel_cau = pprel_cau.strip()
        procs = {}
        if pprel_cau != '':
            procs['ppcause'] = pprel_cau
        if pprel_sp != '' and pprel_ep != '':
            tmprel = [pprel_sp, 'path_to_path', pprel_ep, procs]
            relation_dic['path_to_path']['list'].append(tmprel)

    print('path_to_path relation : ', len(relation_dic['path_to_path']['list']))


    # 3. 证症关联
    ps_info_sheet = workbook.sheet_by_name(ps_info_name)
    for i in range(0, 19, 2):
        tmp_lis = ps_info_sheet.col_values(i)[0:]
        construct_rel(tmp_lis, 'path_to_sym')
    print('path_to_sym relation : ', len(relation_dic['path_to_sym']['list']))

    # 证症关联添加属性

    for psrel_sym, psrel_path, psrel_ori, psrel_mod in zip(psrel_symlis, psrel_pathlis, psrel_orilis, psrel_modlis):
        psrel_sym = psrel_sym.strip()
        psrel_path = psrel_path.strip()
        psrel_ori = psrel_ori.strip()
        psrel_mod = psrel_mod.strip()
        if psrel_sym != '' and psrel_path!='':
            query = [psrel_path, 'path_to_sym', psrel_sym, {}]
            for itemdic in relation_dic['path_to_sym']['list']:
                if query == itemdic:
                    proc = {
                        'ori':psrel_ori,
                        'modern':psrel_mod
                    }
                    itemdic[-1] = proc


    # 4. 证方关系
    pres_info_sheet = workbook.sheet_by_name(pres_info_name)
    for i in range(0, 19, 2):

        tmp_lis = pres_info_sheet.col_values(i)[0:]
        construct_rel(tmp_lis, 'prescribe')

    print('prescribe relation : ', len(relation_dic['prescribe']['list']))


    # 5. 证与膳食关系
    foodall_info_sheet = workbook.sheet_by_name(foodall_info_name)
    for i in range(0, 19, 2):

        tmp_lis = foodall_info_sheet.col_values(i)[0:]
        construct_rel(tmp_lis, 'do_eat')
    print('do_eat relation : ', len(relation_dic['do_eat']['list']))

    # 6. 证与穴位关系
    acupall_info_sheet = workbook.sheet_by_name(acupall_info_name)
    for i in range(0, 19, 2):
        tmp_lis = acupall_info_sheet.col_values(i)[0:]
        construct_rel(tmp_lis, 'do_acupunc')
    print('do_acupunc relation : ', len(relation_dic['do_acupunc']['list']))


    return entity_dic, relation_dic


class MedicalExtractor(object):
    def __init__(self):
        super(MedicalExtractor,self).__init__()
        self.graph = Graph(
            "http:localhost:7474",
            auth=("neo4j","123456")
        )

        # 直接传词典
        self.entity = {}
        self.relation = {}
        self.prospect = {}

    def clear_kg(self):
        self.graph.delete_all()

    def write_nodes(self,entitys,entity_type):
        """
        写入节点
        :param entitys:
        :param entity_type:
        :return:
        """
        print("写入 {0} 实体".format(entity_type))
        for node in tqdm(set(entitys),ncols=80):
            cql = """MERGE(n:{label}{{name:'{entity_name}'}})""".format(
                label=entity_type,entity_name=node.replace("'",""))
            try:
                self.graph.run(cql)
            except Exception as e:
                print(e)
                print(cql)


    def write_edges(self,triples,head_type,tail_type):
        """
        写入边
        :param triples:
        :param head_type:
        :param tail_type:
        :return:
        """
        print("写入 {0} 关系".format(head_type + '——' + tail_type))
        #
        for head,relation,tail,_ in tqdm(triples,ncols=80):
            if _ == {}:
                cql = """MATCH(p:{head_type}),(q:{tail_type}) WHERE p.name='{head}' AND q.name='{tail}'
                        MERGE (p)-[r:{relation}]->(q)""".format(
                            head_type=head_type,tail_type=tail_type,head=head.replace("'",""),
                            tail=tail.replace("'",""),relation=relation)
                try:
                    self.graph.run(cql)
                except Exception as e:
                    print(e)
                    print(cql)


    def write_node_attributes(self,entity_infos, etype):
        """
        写入实体属性
        :param entity_infos:
        :param etype:
        :return:
        """
        print("写入 {0} 实体的属性".format(etype))

        for e_dict in tqdm(entity_infos,ncols=80):

            name = e_dict['name']
            attr = e_dict['attr']

            for k,v in attr.items():

                cql = """MATCH (n:{label})
                    WHERE n.name='{name}'
                    set n.{k}='{v}'""".format(label=etype,name=name,k=k,v=v)
                try:
                    self.graph.run(cql)
                except Exception as e:
                    print(e)
                    print(cql)

    def write_rel_attributes(self,triples,head_type,tail_type):
        """
        写入关系属性
        :param entity_infos:
        :param etype:
        :return:
        """
        print("写入 {0} 关系的属性".format(head_type + '——>' + tail_type))

        for head,relation,tail,procs in tqdm(triples,ncols=80):

            if procs is None:
                continue
            string = ''
            for type, val in procs.items():
                if string == '':
                    string += """{type}:'{val}'""".format(type=type, val=val)
                else:
                    string += """,{type}:'{val}'""".format(type=type, val=val)

            cql = """MATCH(p:{head_type}),(q:{tail_type}) WHERE p.name='{head}' AND q.name='{tail}'
            MERGE (p)-[r:{relation} {{{attribute}}}]->(q)""".format(
                        head_type=head_type,tail_type=tail_type,head=head.replace("'",""),
                        tail=tail.replace("'",""),relation=relation, attribute=string)
            try:
                self.graph.run(cql)
            except Exception as e:
                print(e)
                print(cql)



    def create_entitis(self):
        for entkey in self.entity:
            print("write entity : ", entkey)
            nodelist = [dic['name'] for dic in self.entity[entkey]]
            self.write_nodes(nodelist, entkey)

    def create_relations(self):
        for relkey in self.relation:
            print("write relation : ", relkey)
            self.write_edges(self.relation[relkey]['list'],self.relation[relkey]['head_type'], self.relation[relkey]['tail_type'])


    def set_entity_attributes(self):
        for entkey in self.entity:
            print("write entity attribute : ", entkey)
            self.write_node_attributes(self.entity[entkey],entkey)


    def set_rel_attributes(self):
        for relkey in self.relation:
            print("write relation attribute : ", relkey)
            self.write_rel_attributes(self.relation[relkey]['list'],self.relation[relkey]['head_type'], self.relation[relkey]['tail_type'])


    def load_dic(self, entity, relation):         #导入实体、关系、属性
        self.entity = entity
        self.relation = relation





if __name__ == '__main__':
    basegraph_path = './knowledge_exl/11_graph.xlsx'
    relarre_path = './knowledge_exl/11_rel_attr.xlsx'
    nodeattr_path = './knowledge_exl/11_node_attr.xlsx'
    entjsonpath = './knowledge_exl/entity.json'
    reljsonpath = './knowledge_exl/relation.json'
    entity_dic, relation_dic = load_kg_excel(basegraph_path, relarre_path, nodeattr_path)
    entjsonfile = open(entjsonpath, 'w', encoding='utf8')
    json.dump(entity_dic, entjsonfile, ensure_ascii=False)
    reljsonfile = open(reljsonpath, 'w', encoding='utf8')
    json.dump(relation_dic, reljsonfile, ensure_ascii=False)
    extractor = MedicalExtractor()
    extractor.clear_kg()
    extractor.load_dic(entity_dic, relation_dic)
    extractor.create_entitis()
    extractor.create_relations()
    extractor.set_entity_attributes()
    extractor.set_rel_attributes()
    #

