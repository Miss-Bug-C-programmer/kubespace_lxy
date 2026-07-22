// Code generated-style manual deepcopy implementations. DO NOT EDIT casually.
package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

func (in *SpaceLinkSnapshot) DeepCopy() *SpaceLinkSnapshot {
	if in == nil {
		return nil
	}
	out := *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec.Windows = append([]ContactWindow(nil), in.Spec.Windows...)
	out.Status.History = append([]LinkHistoryEntry(nil), in.Status.History...)
	out.Status.Conditions = append([]metav1.Condition(nil), in.Status.Conditions...)
	return &out
}

func (in *SpaceLinkSnapshotList) DeepCopy() *SpaceLinkSnapshotList {
	if in == nil {
		return nil
	}
	out := *in
	out.Items = make([]SpaceLinkSnapshot, len(in.Items))
	for i := range in.Items {
		out.Items[i] = *in.Items[i].DeepCopy()
	}
	return &out
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyCapabilities(in []CapabilityRequirement) []CapabilityRequirement {
	out := make([]CapabilityRequirement, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Precision = append([]string(nil), in[i].Precision...)
		out[i].Software = copyStringMap(in[i].Software)
	}
	return out
}

func (in *SpaceMission) DeepCopy() *SpaceMission {
	if in == nil {
		return nil
	}
	out := *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec.RequiredCapabilities = copyCapabilities(in.Spec.RequiredCapabilities)
	out.Spec.AlternativeCapabilities = make([]CapabilitySet, len(in.Spec.AlternativeCapabilities))
	for i := range in.Spec.AlternativeCapabilities {
		out.Spec.AlternativeCapabilities[i] = in.Spec.AlternativeCapabilities[i]
		out.Spec.AlternativeCapabilities[i].AllOf = copyCapabilities(in.Spec.AlternativeCapabilities[i].AllOf)
	}
	out.Spec.RequiredSoftware = copyStringMap(in.Spec.RequiredSoftware)
	out.Spec.Inputs = make([]DataObject, len(in.Spec.Inputs))
	for i := range in.Spec.Inputs {
		out.Spec.Inputs[i] = in.Spec.Inputs[i]
		out.Spec.Inputs[i].Locations = append([]string(nil), in.Spec.Inputs[i].Locations...)
	}
	out.Spec.ResultDestinations = append([]string(nil), in.Spec.ResultDestinations...)
	out.Spec.WorkloadTemplate = *in.Spec.WorkloadTemplate.DeepCopy()
	out.Status.Conditions = append([]metav1.Condition(nil), in.Status.Conditions...)
	return &out
}

func (in *SpaceMissionList) DeepCopy() *SpaceMissionList {
	if in == nil {
		return nil
	}
	out := *in
	out.Items = make([]SpaceMission, len(in.Items))
	for i := range in.Items {
		out.Items[i] = *in.Items[i].DeepCopy()
	}
	return &out
}

func (in *SpaceDomainResourceSummary) DeepCopy() *SpaceDomainResourceSummary {
	if in == nil {
		return nil
	}
	out := *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec.Devices = make([]DeviceCapacity, len(in.Spec.Devices))
	for i := range in.Spec.Devices {
		out.Spec.Devices[i] = in.Spec.Devices[i]
		out.Spec.Devices[i].Architectures = append([]string(nil), in.Spec.Devices[i].Architectures...)
		out.Spec.Devices[i].Models = append([]string(nil), in.Spec.Devices[i].Models...)
		out.Spec.Devices[i].Precision = append([]string(nil), in.Spec.Devices[i].Precision...)
	}
	out.Spec.Software = copyStringMap(in.Spec.Software)
	out.Spec.DataLocations = append([]string(nil), in.Spec.DataLocations...)
	out.Status.Conditions = append([]metav1.Condition(nil), in.Status.Conditions...)
	return &out
}

func (in *SpaceDomainResourceSummaryList) DeepCopy() *SpaceDomainResourceSummaryList {
	if in == nil {
		return nil
	}
	out := *in
	out.Items = make([]SpaceDomainResourceSummary, len(in.Items))
	for i := range in.Items {
		out.Items[i] = *in.Items[i].DeepCopy()
	}
	return &out
}

func (in *SpacePlacementIntent) DeepCopy() *SpacePlacementIntent {
	if in == nil {
		return nil
	}
	out := *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Spec.InputTransfers = append([]TransferEpoch(nil), in.Spec.InputTransfers...)
	if in.Spec.ResultTransfer != nil {
		v := *in.Spec.ResultTransfer
		out.Spec.ResultTransfer = &v
	}
	out.Spec.SnapshotSequences = make(map[string]int64, len(in.Spec.SnapshotSequences))
	for k, v := range in.Spec.SnapshotSequences {
		out.Spec.SnapshotSequences[k] = v
	}
	out.Spec.Explanations = append([]ConstraintExplanation(nil), in.Spec.Explanations...)
	if in.Status.ActivePod != nil {
		v := *in.Status.ActivePod
		out.Status.ActivePod = &v
	}
	if in.Status.LastObservation != nil {
		v := *in.Status.LastObservation
		out.Status.LastObservation = &v
	}
	out.Status.Conditions = append([]metav1.Condition(nil), in.Status.Conditions...)
	return &out
}

func (in *SpacePlacementIntentList) DeepCopy() *SpacePlacementIntentList {
	if in == nil {
		return nil
	}
	out := *in
	out.Items = make([]SpacePlacementIntent, len(in.Items))
	for i := range in.Items {
		out.Items[i] = *in.Items[i].DeepCopy()
	}
	return &out
}
