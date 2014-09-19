import json
from math import log

# Make an Expectation Maximization answer for a task
def make_em_answer(task_obj, model_spec):

    example_to_worker_label = {}
    worker_to_example_label = {}
    label_set=[]
    answers = []

    # Label set
    label_set = []

    # Build up initial variables for em
    responses = model_spec.response_model.objects.filter(
        task__task_type=task_obj.task_type)
    for response in responses :

            answer_list = json.loads(response.content)
            for point_id in answer_list.keys() :

                worker_id = response.worker.worker_id
                unique_id = point_id
                current_label = answer_list[point_id]

                example_to_worker_label.setdefault(unique_id, []).append(
                    (worker_id, current_label))
                worker_to_example_label.setdefault(worker_id, []).append(
                    (unique_id, current_label))

                if current_label not in label_set :
                    label_set.append(current_label)

    # EM algorithm
    iterations = 20

    ans, b, c = EM(example_to_worker_label,
                   worker_to_example_label,
                   label_set).ExpectationMaximization(iterations)

    # Gather answer
    point_ids = json.loads(task_obj.responses.all()[0].content).keys()
    answer_label = {}

    for point_id in point_ids :
        unique_id = point_id
        soft_label = ans[unique_id]
        maxv = 0
        cur_label = label_set[0]
        for label, weight in soft_label.items() :
            if weight > maxv :
                maxv = weight
                cur_label = label
        answer_label[point_id] = float(cur_label)

    return json.dumps(answer_label)


class EM:
    def __init__(self,example_to_worker_label,worker_to_example_label,label_set):
        self.example_to_worker_label=example_to_worker_label
        self.worker_to_example_label=worker_to_example_label
        self.label_set=label_set

    def ConfusionMatrix(self, worker_to_example_label, example_to_softlabel):
        worker_to_finallabel_weight = {}
        worker_to_finallabel_workerlabel_weight = {}

        for worker, example_label in worker_to_example_label.items():
            if worker not in worker_to_finallabel_weight:
                worker_to_finallabel_weight[worker] = {}
            if worker not in worker_to_finallabel_workerlabel_weight:
                worker_to_finallabel_workerlabel_weight[worker] = {}
            for example, workerlabel in example_label:
                softlabel =  example_to_softlabel[example]
                for finallabel, weight in softlabel.items():
                    worker_to_finallabel_weight[worker][finallabel] = worker_to_finallabel_weight[worker].get(finallabel, 0)+weight
                    if finallabel not in worker_to_finallabel_workerlabel_weight[worker]:
                        worker_to_finallabel_workerlabel_weight[worker][finallabel] = {}
                    worker_to_finallabel_workerlabel_weight[worker][finallabel][workerlabel] = worker_to_finallabel_workerlabel_weight[worker][finallabel].get(workerlabel, 0)+weight


        worker_to_confusion_matrix = worker_to_finallabel_workerlabel_weight
        for worker, finallabel_workerlabel_weight in worker_to_finallabel_workerlabel_weight.items():
            for finallabel, workerlabel_weight in finallabel_workerlabel_weight.items():
                if worker_to_finallabel_weight[worker][finallabel] == 0:
                    #approximately no possibility
                    for label in self.label_set:
                        if label==finallabel:
                            worker_to_confusion_matrix[worker][finallabel][label]=0.7
                        else:
                            worker_to_confusion_matrix[worker][finallabel][label]=0.3/(len(self.label_set)-1)
                else:
                    for label in self.label_set:
                        if label in workerlabel_weight:
                            worker_to_confusion_matrix[worker][finallabel][label] = workerlabel_weight[label]*1.0/worker_to_finallabel_weight[worker][finallabel]
                        else:
                            worker_to_confusion_matrix[worker][finallabel][label] = 0.0

        return worker_to_confusion_matrix

    def PriorityProbability(self, example_to_softlabel):
        label_to_priority_probability = {}
        for _, softlabel in example_to_softlabel.items():
            for label, probability in softlabel.items():
                label_to_priority_probability[label] = label_to_priority_probability.get(label,0)+probability
        for label, count in label_to_priority_probability.items():
            label_to_priority_probability[label] = count*1.0/len(example_to_softlabel)
        return label_to_priority_probability


    def ProbabilityMajorityVote(self, example_to_worker_label, label_to_priority_probability, worker_to_confusion_matrix):
        example_to_sortlabel = {}
        for example, worker_label_set in example_to_worker_label.items():
            sortlabel = {}
            total_weight = 0
            # can use worker
            for final_label, priority_probability in label_to_priority_probability.items():
                weight = priority_probability
                for (worker, worker_label) in worker_label_set:
                    weight *= worker_to_confusion_matrix[worker][final_label][worker_label]
                total_weight += weight
                sortlabel[final_label] = weight
            for final_label, weight in sortlabel.items():
                if total_weight == 0:
                    assert weight == 0
                    #approximately less probability
                    sortlabel[final_label]=1.0/len(self.label_set)
                else:
                    sortlabel[final_label] = weight*1.0/total_weight
            example_to_sortlabel[example] = sortlabel
        return example_to_sortlabel


#Pj
    def InitPriorityProbability(self, label_set):
        label_to_priority_probability = {}
        for label in label_set:
            label_to_priority_probability[label] = 1.0/len(label_set)
        return label_to_priority_probability
#Pi
    def InitConfusionMatrix(self, workers, label_set):
        worker_to_confusion_matrix = {}
        for worker in workers:
            if worker not in worker_to_confusion_matrix:
                worker_to_confusion_matrix[worker] = {}
            for label1 in label_set:
                if label1 not in worker_to_confusion_matrix[worker]:
                    worker_to_confusion_matrix[worker][label1] = {}
                for label2 in label_set:
                    if label1 == label2:
                        worker_to_confusion_matrix[worker][label1][label2] = 0.7
                    else:
                        worker_to_confusion_matrix[worker][label1][label2] = 0.3/(len(label_set)-1)
        return worker_to_confusion_matrix

    def ExpectationMaximization(self, iterr = 10):
        example_to_worker_label = self.example_to_worker_label
        worker_to_example_label = self.worker_to_example_label
        label_set = self.label_set

        label_to_priority_probability = self.InitPriorityProbability(label_set)
        worker_to_confusion_matrix = self.InitConfusionMatrix(worker_to_example_label.keys(), label_set)
        while iterr>0:
            example_to_softlabel = self.ProbabilityMajorityVote(example_to_worker_label, label_to_priority_probability, worker_to_confusion_matrix)

            label_to_priority_probability = self.PriorityProbability(example_to_softlabel)
            worker_to_confusion_matrix = self.ConfusionMatrix(worker_to_example_label, example_to_softlabel)

            # compute the likelihood
            #lh=self.computelikelihood(worker_to_confusion_matrix,label_to_priority_probability,example_to_worker_label); # can be omitted
            #print alliter-iterr,':',lh;
            #print alliter-iterr,'\t',lh-prelh
            iterr -= 1

        return example_to_softlabel,label_to_priority_probability,worker_to_confusion_matrix

    def computelikelihood(self,w2cm,l2pd,e2wl):
        lh=0;
        for _,wl in e2wl.items():
            temp=0;
            for truelabel,prior in l2pd.items():
                inner=1;
                for workerlabel in wl:
                    worker=workerlabel[0]
                    label=workerlabel[1]
                    inner*=w2cm[worker][truelabel][label]
                temp+=inner*prior
            lh+=log(temp)
        return lh


def getaccuracy(e2lpd,label_set):
    accurate=0
    allexamples=0

    for example in e2lpd.keys():

        distribution=e2lpd[example]
        maxlabel=0
        maxxvalue=-1
        for label in label_set:
            if maxxvalue<=distribution[label]:
                maxlabel=label
                maxxvalue=distribution[label]
        truelabel=example.split('_')[1]
        if maxlabel==truelabel:
            accurate+=1

        allexamples+=1

    return accurate*1.0/allexamples

def gete2wlandw2el(filename):
    example_to_worker_label = {}
    worker_to_example_label = {}
    label_set=[]

    f = open(filename)
    for line in f.xreadlines():
        line = line.strip()
        if not line:
            continue
        items =  line.split("\t")

        example_to_worker_label.setdefault(items[1], []).append((items[0], items[2]))
        worker_to_example_label.setdefault(items[0], []).append((items[1], items[2]))

        if items[2] not in label_set:
            label_set.append(items[2])

    return example_to_worker_label,worker_to_example_label,label_set


#if __name__ == "__main__":

#    filename=r'filename'
#    example_to_worker_label,worker_to_example_label,label_set=gete2wlandw2el(filename)
#    iterations=20 # EM iteration number
#    EM(example_to_worker_label,worker_to_example_label,label_set).ExpectationMaximization(iterations)
