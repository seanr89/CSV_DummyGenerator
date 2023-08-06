
public class DataModel
{
    public Guid customerId { get; set; }
    public Guid offerId { get; set; }
    public DateTime startDate { get; set; }
    public DateTime endDate { get; set; }
    public DateTime? activation { get; set; }
}