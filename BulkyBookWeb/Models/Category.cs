using System.ComponentModel.DataAnnotations; 

namespace BulkyBookWeb.Models
{
    public class Category
    {
        [Key]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; }

        [Display(Name = "Display Order")]
        [Range(1,100,ErrorMessage = "Display order has to be between 1 and 100, silly!! :)")]
        public int DisplayOrder { get; set; }
        public DateTime CreatedDate { get; set; } = DateTime.Now;
    }
}
